# dags/operacoes.py
from __future__ import annotations

import io
import re
from datetime import datetime

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import s3fs

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator


# ---------- Config (Airflow Variables) ----------
S3_ACCESS_KEY = Variable.get("S3_ACCESS_KEY").strip()
S3_SECRET_KEY = Variable.get("S3_SECRET_KEY").strip()
S3_ENDPOINT = Variable.get(
    "S3_ENDPOINT",
    default_var="http://minio.minio.svc.cluster.local:9000",
).strip()

BUCKET = Variable.get("TD_BUCKET", default_var="analytics").strip()

RAW_PREFIX = Variable.get("TD_RAW_PREFIX", default_var="raw/tesouro_direto/operacoes/").strip()
STG_PREFIX = Variable.get("TD_STG_PREFIX", default_var="staging/tesouro_direto/operacoes_parquet/").strip()

# 0 = processa tudo
ONLY_NEWER_THAN_DAYS = int(Variable.get("TD_ONLY_NEWER_THAN_DAYS", default_var="0"))


# ---------- Helpers ----------
def snake_case(s: str) -> str:
    s = (s or "").strip()
    s = s.replace("º", "o").replace("ª", "a")
    s = re.sub(r"[^\w]+", "_", s, flags=re.UNICODE)
    s = re.sub(r"__+", "_", s)
    return s.strip("_").lower()


def get_fs() -> s3fs.S3FileSystem:
    # MinIO: força signature v4 e endpoint custom
    return s3fs.S3FileSystem(
        key=S3_ACCESS_KEY,
        secret=S3_SECRET_KEY,
        client_kwargs={"endpoint_url": S3_ENDPOINT},
        config_kwargs={"signature_version": "s3v4"},
    )


def list_csv_files(fs: s3fs.S3FileSystem, bucket: str, prefix: str) -> list[str]:
    base = f"{bucket}/{prefix}".rstrip("/") + "/"
    files = fs.glob(base + "*.csv")
    return sorted(files)


def should_process(fs: s3fs.S3FileSystem, path: str) -> bool:
    if ONLY_NEWER_THAN_DAYS <= 0:
        return True

    info = fs.info(path)
    mtime = info.get("LastModified") or info.get("last_modified") or info.get("mtime")
    if mtime is None:
        return True

    # best-effort timestamp
    try:
        ts = mtime.timestamp()
    except Exception:
        return True

    cutoff = datetime.utcnow().timestamp() - ONLY_NEWER_THAN_DAYS * 86400
    return ts >= cutoff


def normalize_for_arrow(df: pd.DataFrame) -> pd.DataFrame:
    # datas (sem timezone)
    for c in ["data_da_operacao", "vencimento_do_titulo"]:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce").dt.tz_localize(None)

    # strings
    for c in ["codigo_do_investidor", "tipo_titulo", "tipo_da_operacao", "canal_da_operacao"]:
        if c in df.columns:
            df[c] = df[c].astype("string")

    # números (double)
    for c in ["quantidade", "valor_do_titulo", "valor_da_operacao"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("float64")

    # partições (garante que não vira dtype problemático pro pyarrow)
    for c in ["op_year", "op_month"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64").astype("float64")

    return df


def transform_and_write_parquet(**context) -> None:
    fs = get_fs()

    in_files = list_csv_files(fs, BUCKET, RAW_PREFIX)
    if not in_files:
        print(f"Nenhum CSV encontrado em s3://{BUCKET}/{RAW_PREFIX}")
        return

    print("Config:")
    print("  S3_ENDPOINT =", S3_ENDPOINT)
    print("  BUCKET =", BUCKET)
    print("  RAW_PREFIX =", RAW_PREFIX)
    print("  STG_PREFIX =", STG_PREFIX)
    print("  files =", len(in_files))

    for in_path in in_files:
        if not should_process(fs, in_path):
            continue

        filename = in_path.split("/")[-1]
        print(f"\nProcessando: s3://{in_path}")

        # --- read CSV (pt-BR, ;, latin-1) ---
        with fs.open(in_path, "rb") as f:
            raw = f.read()

        df = pd.read_csv(
            io.BytesIO(raw),
            sep=";",
            encoding="latin-1",
            dtype=str,  # primeiro como string
        )

        # --- normalize headers ---
        df.columns = [snake_case(c) for c in df.columns]

        # --- rename to friendly final names (opcional, mas ajuda) ---
        # mapeia nomes comuns do dataset
        rename_map = {
            "codigo_do_investidor": "codigo_do_investidor",
            "data_da_operacao": "data_da_operacao",
            "tipo_titulo": "tipo_titulo",
            "vencimento_do_titulo": "vencimento_do_titulo",
            "quantidade": "quantidade",
            "valor_do_titulo": "valor_do_titulo",
            "valor_da_operacao": "valor_da_operacao",
            "tipo_da_operacao": "tipo_da_operacao",
            "canal_da_operacao": "canal_da_operacao",
        }
        df = df.rename(columns=rename_map)

        if "data_da_operacao" not in df.columns:
            raise ValueError(f"Coluna 'Data da Operacao' não encontrada. Colunas: {list(df.columns)}")

        # --- parse dates ---
        df["data_da_operacao"] = pd.to_datetime(df["data_da_operacao"], format="%d/%m/%Y", errors="coerce")
        if "vencimento_do_titulo" in df.columns:
            df["vencimento_do_titulo"] = pd.to_datetime(df["vencimento_do_titulo"], format="%d/%m/%Y", errors="coerce")

        # --- parse numbers with pt-BR decimals ---
        def to_float(col: str) -> None:
            if col in df.columns:
                s = df[col].astype(str)
                s = s.str.replace(".", "", regex=False)   # milhar
                s = s.str.replace(",", ".", regex=False)  # decimal
                df[col] = pd.to_numeric(s, errors="coerce")

        to_float("quantidade")
        to_float("valor_do_titulo")
        to_float("valor_da_operacao")

        # remove linhas sem data
        df = df.dropna(subset=["data_da_operacao"])

        # --- derive partitions (year/month) from data_da_operacao ---
        df["op_year"] = df["data_da_operacao"].dt.year
        df["op_month"] = df["data_da_operacao"].dt.month

        # --- final normalize for pyarrow ---
        df = normalize_for_arrow(df)

        # debug leve (ajuda se quebrar de novo)
        print("dtypes:")
        print(df.dtypes)

        # cria table com safe=False para evitar erro por casts “estritos”
        table = pa.Table.from_pandas(df, preserve_index=False, safe=False)

        # --- write parquet partitioned by year/month ---
        out_root = f"{BUCKET}/{STG_PREFIX}".rstrip("/")

        pq.write_to_dataset(
            table,
            root_path=out_root,
            filesystem=fs,
            partition_cols=["op_year", "op_month"],
            basename_template=filename.replace(".csv", "") + "-{i}.parquet",
            use_dictionary=True,
            compression="snappy",
        )

        print(f"OK -> s3://{out_root} (partitioned op_year/op_month)")


with DAG(
    dag_id="td_operacoes_raw_to_parquet",
    start_date=datetime(2026, 1, 1),
    schedule=None,  # on-demand
    catchup=False,
    tags=["tesouro-direto", "raw-to-parquet"],
    default_args={"owner": "airflow", "retries": 1},
) as dag:
    raw_to_parquet = PythonOperator(
        task_id="raw_csv_to_parquet_partitioned",
        python_callable=transform_and_write_parquet,
    )

    raw_to_parquet
