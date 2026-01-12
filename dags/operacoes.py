# dags/td_operacoes_raw_to_parquet.py
from __future__ import annotations

import io
import os
import re
from datetime import datetime

import pandas as pd
import pyarrow as pas
import pyarrow.parquet as pq
import s3fs

from airflow import DAG
from airflow.operators.python import PythonOperator


# ---------- Config ----------
S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "http://minio.minio.svc.cluster.local:9000")
S3_ACCESS_KEY = os.environ["S3_ACCESS_KEY"]
S3_SECRET_KEY = os.environ["S3_SECRET_KEY"]

BUCKET = os.environ.get("TD_BUCKET", "analytics")

RAW_PREFIX = os.environ.get("TD_RAW_PREFIX", "raw/tesouro_direto/operacoes/")
STG_PREFIX = os.environ.get("TD_STG_PREFIX", "staging/tesouro_direto/operacoes_parquet/")

# opcional: processar só arquivos recentes (dias)
ONLY_NEWER_THAN_DAYS = int(os.environ.get("TD_ONLY_NEWER_THAN_DAYS", "0"))

# ---------- Helpers ----------
def snake_case(s: str) -> str:
    s = (s or "").strip()
    s = s.replace("º", "o").replace("ª", "a")
    s = re.sub(r"[^\w]+", "_", s, flags=re.UNICODE)
    s = re.sub(r"__+", "_", s)
    return s.strip("_").lower()


def get_fs() -> s3fs.S3FileSystem:
    return s3fs.S3FileSystem(
        key=S3_ACCESS_KEY,
        secret=S3_SECRET_KEY,
        client_kwargs={"endpoint_url": S3_ENDPOINT},
    )


def list_csv_files(fs: s3fs.S3FileSystem, bucket: str, prefix: str) -> list[str]:
    base = f"{bucket}/{prefix}".rstrip("/") + "/"
    # s3fs expects paths without s3://
    files = fs.glob(base + "*.csv")
    return sorted(files)


def should_process(fs: s3fs.S3FileSystem, path: str) -> bool:
    if ONLY_NEWER_THAN_DAYS <= 0:
        return True
    info = fs.info(path)
    # mtime may be string or datetime-like depending on backend
    mtime = info.get("LastModified") or info.get("last_modified") or info.get("mtime")
    if mtime is None:
        return True
    if isinstance(mtime, str):
        # best-effort parse
        try:
            mtime = datetime.fromisoformat(mtime.replace("Z", "+00:00"))
        except Exception:
            return True
    cutoff = datetime.utcnow().timestamp() - ONLY_NEWER_THAN_DAYS * 86400
    try:
        ts = mtime.timestamp()
    except Exception:
        return True
    return ts >= cutoff


def transform_and_write_parquet(**context) -> None:
    fs = get_fs()
    in_files = list_csv_files(fs, BUCKET, RAW_PREFIX)

    if not in_files:
        print(f"Nenhum CSV encontrado em s3://{BUCKET}/{RAW_PREFIX}")
        return

    for in_path in in_files:
        if not should_process(fs, in_path):
            continue

        filename = os.path.basename(in_path)  # ex: operacoes_2015-07.csv
        print(f"Processando: s3://{in_path}")

        # --- read CSV (pt-BR, ;, latin-1) ---
        with fs.open(in_path, "rb") as f:
            raw = f.read()

        df = pd.read_csv(
            io.BytesIO(raw),
            sep=";",
            encoding="latin-1",
            dtype=str,  # primeiro como string para evitar dor de cabeça
        )

        # --- normalize headers ---
        df.columns = [snake_case(c) for c in df.columns]

        # expected columns (from your sample) after snake_case:
        # codigo_do_investidor, data_da_operacao, tipo_titulo, vencimento_do_titulo,
        # quantidade, valor_do_titulo, valor_da_operacao, tipo_da_operacao, canal_da_operacao

        # --- parse / cast ---
        if "data_da_operacao" in df.columns:
            df["data_da_operacao"] = pd.to_datetime(df["data_da_operacao"], format="%d/%m/%Y", errors="coerce")
        if "vencimento_do_titulo" in df.columns:
            df["vencimento_do_titulo"] = pd.to_datetime(df["vencimento_do_titulo"], format="%d/%m/%Y", errors="coerce")

        # números com vírgula decimal
        def to_float(col: str) -> None:
            if col in df.columns:
                df[col] = (
                    df[col]
                    .astype(str)
                    .str.replace(".", "", regex=False)   # se vier separador de milhar
                    .str.replace(",", ".", regex=False)  # decimal pt-BR -> en-US
                )
                df[col] = pd.to_numeric(df[col], errors="coerce")

        to_float("quantidade")
        to_float("valor_do_titulo")
        to_float("valor_da_operacao")

        # strings úteis
        for col in ["codigo_do_investidor", "tipo_titulo", "tipo_da_operacao", "canal_da_operacao"]:
            if col in df.columns:
                df[col] = df[col].astype("string")

        # --- derive partitions (year/month) from data_da_operacao ---
        if "data_da_operacao" not in df.columns:
            raise ValueError("Coluna 'data_da_operacao' não encontrada após normalização.")

        df = df.dropna(subset=["data_da_operacao"])
        df["op_year"] = df["data_da_operacao"].dt.year.astype("int32")
        df["op_month"] = df["data_da_operacao"].dt.month.astype("int8")

        # --- write parquet partitioned by year/month ---
        # output path example:
        # s3://analytics/staging/tesouro_direto/operacoes_parquet/op_year=2015/op_month=7/operacoes_2015-07.parquet
        # using pyarrow for stable partitioning
        table = pa.Table.from_pandas(df, preserve_index=False)

        out_dir = f"{BUCKET}/{STG_PREFIX}".rstrip("/")  # no s3://
        ensure_dirs = True  # s3 doesn't need, but keeps intention

        # write_dataset partitions
        pq.write_to_dataset(
            table,
            root_path=out_dir,
            filesystem=fs,
            partition_cols=["op_year", "op_month"],
            basename_template=filename.replace(".csv", "") + "-{i}.parquet",
            use_dictionary=True,
            compression="snappy",
        )

        print(f"OK -> s3://{out_dir} (partitioned op_year/op_month)")


# ---------- DAG ----------
with DAG(
    dag_id="td_operacoes_raw_to_parquet",
    start_date=datetime(2026, 1, 1),
    schedule=None,  # manual / on-demand
    catchup=False,
    tags=["tesouro-direto", "raw-to-parquet"],
    default_args={"owner": "airflow", "retries": 1},
) as dag:
    raw_to_parquet = PythonOperator(
        task_id="raw_csv_to_parquet_partitioned",
        python_callable=transform_and_write_parquet,
    )

    raw_to_parquet
