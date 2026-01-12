# dags/td_operacoes_taskflow.py
from __future__ import annotations

import io
import re
from datetime import datetime, date
from typing import List, Dict

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import s3fs
import trino

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable


# ---------- Config (Airflow Variables) ----------
S3_ACCESS_KEY = Variable.get("S3_ACCESS_KEY").strip()
S3_SECRET_KEY = Variable.get("S3_SECRET_KEY").strip()
S3_ENDPOINT = Variable.get("S3_ENDPOINT", default_var="http://minio.minio.svc.cluster.local:9000").strip()

BUCKET = Variable.get("TD_BUCKET", default_var="analytics").strip()
RAW_PREFIX = Variable.get("TD_RAW_PREFIX", default_var="raw/tesouro_direto/operacoes/").strip()

TMP_PREFIX = Variable.get("TD_TMP_PREFIX", default_var="tmp/tesouro_direto/operacoes_parquet/").strip()

TRINO_HOST = Variable.get("TRINO_HOST", default_var="trino-coordinator.analytics.svc.cluster.local").strip()
TRINO_PORT = int(Variable.get("TRINO_PORT", default_var="8080"))
TRINO_USER = Variable.get("TRINO_USER", default_var="airflow").strip()

ICEBERG_CATALOG = Variable.get("ICEBERG_CATALOG", default_var="iceberg").strip()
ICEBERG_SCHEMA = Variable.get("ICEBERG_SCHEMA", default_var="analytics").strip()
ICEBERG_TABLE = Variable.get("ICEBERG_TABLE", default_var="td_operacoes").strip()

ONLY_NEWER_THAN_DAYS = int(Variable.get("TD_ONLY_NEWER_THAN_DAYS", default_var="0"))


# ---------- Helpers ----------
def snake_case(s: str) -> str:
    s = (s or "").strip()
    s = s.replace("º", "o").replace("ª", "a")
    s = re.sub(r"[^\w]+", "_", s, flags=re.UNICODE)
    s = re.sub(r"__+", "_", s)
    return s.strip("_").lower()


def parse_month_from_filename(filename: str) -> tuple[int, int]:
    m = re.search(r"(\d{4})-(\d{2})", filename)
    if not m:
        raise ValueError(f"Não consegui extrair YYYY-MM do filename: {filename}")
    return int(m.group(1)), int(m.group(2))


def month_range(year: int, month: int) -> tuple[date, date]:
    start = date(year, month, 1)
    if month == 12:
        end = date(year + 1, 1, 1)
    else:
        end = date(year, month + 1, 1)
    return start, end


def get_fs() -> s3fs.S3FileSystem:
    return s3fs.S3FileSystem(
        key=S3_ACCESS_KEY,
        secret=S3_SECRET_KEY,
        client_kwargs={"endpoint_url": S3_ENDPOINT},
        config_kwargs={"signature_version": "s3v4"},
    )


def connect_trino() -> trino.dbapi.Connection:
    return trino.dbapi.connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user=TRINO_USER,
        http_scheme="http",
    )


def ensure_iceberg_table(cur) -> None:
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {ICEBERG_CATALOG}.{ICEBERG_SCHEMA}")

    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {ICEBERG_CATALOG}.{ICEBERG_SCHEMA}.{ICEBERG_TABLE} (
          codigo_do_investidor VARCHAR,
          data_da_operacao DATE,
          tipo_titulo VARCHAR,
          vencimento_do_titulo DATE,
          quantidade DOUBLE,
          valor_do_titulo DOUBLE,
          valor_da_operacao DOUBLE,
          tipo_da_operacao VARCHAR,
          canal_da_operacao VARCHAR
        )
        WITH (
          format = 'PARQUET',
          partitioning = ARRAY['month(data_da_operacao)']
        )
        """
    )


def transform_csv_bytes(raw: bytes) -> pa.Table:
    df = pd.read_csv(io.BytesIO(raw), sep=";", encoding="latin-1", dtype=str)
    df.columns = [snake_case(c) for c in df.columns]

    if "data_da_operacao" not in df.columns:
        raise ValueError(f"Coluna 'Data da Operacao' não encontrada. Colunas: {list(df.columns)}")

    df["data_da_operacao"] = pd.to_datetime(df["data_da_operacao"], format="%d/%m/%Y", errors="coerce")
    if "vencimento_do_titulo" in df.columns:
        df["vencimento_do_titulo"] = pd.to_datetime(df["vencimento_do_titulo"], format="%d/%m/%Y", errors="coerce")

    def to_float(col: str) -> None:
        if col in df.columns:
            s = df[col].astype(str).str.replace(".", "", regex=False).str.replace(",", ".", regex=False)
            df[col] = pd.to_numeric(s, errors="coerce").astype("float64")

    to_float("quantidade")
    to_float("valor_do_titulo")
    to_float("valor_da_operacao")

    # strings
    for col in ["codigo_do_investidor", "tipo_titulo", "tipo_da_operacao", "canal_da_operacao"]:
        if col in df.columns:
            df[col] = df[col].astype("string")

    # datas -> date
    df["data_da_operacao"] = pd.to_datetime(df["data_da_operacao"], errors="coerce").dt.date
    if "vencimento_do_titulo" in df.columns:
        df["vencimento_do_titulo"] = pd.to_datetime(df["vencimento_do_titulo"], errors="coerce").dt.date

    df = df.dropna(subset=["data_da_operacao"])

    cols = [
        "codigo_do_investidor",
        "data_da_operacao",
        "tipo_titulo",
        "vencimento_do_titulo",
        "quantidade",
        "valor_do_titulo",
        "valor_da_operacao",
        "tipo_da_operacao",
        "canal_da_operacao",
    ]
    for c in cols:
        if c not in df.columns:
            df[c] = pd.NA

    df = df[cols]
    return pa.Table.from_pandas(df, preserve_index=False, safe=False)


with DAG(
    dag_id="td_operacoes_raw_to_iceberg_taskflow",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["tesouro-direto", "taskflow", "iceberg"],
    default_args={"owner": "airflow", "retries": 1},
) as dag:

    @task
    def discover_files() -> List[Dict]:
        fs = get_fs()
        base = f"{BUCKET}/{RAW_PREFIX}".rstrip("/") + "/"
        files = sorted(fs.glob(base + "*.csv"))
        if not files:
            return []

        out = []
        for p in files:
            filename = p.split("/")[-1]
            year, month = parse_month_from_filename(filename)
            out.append({"path": p, "filename": filename, "year": year, "month": month})
        return out

    @task
    def extract_transform_to_tmp(fileinfo: Dict) -> Dict:
        fs = get_fs()
        path = fileinfo["path"]
        filename = fileinfo["filename"]
        year = int(fileinfo["year"])
        month = int(fileinfo["month"])

        with fs.open(path, "rb") as f:
            raw = f.read()

        table = transform_csv_bytes(raw)

        tmp_key = f"{TMP_PREFIX}".rstrip("/") + f"/year={year:04d}/month={month:02d}/{filename.replace('.csv','')}.parquet"
        tmp_path = f"{BUCKET}/{tmp_key}"

        # escreve um único parquet por arquivo
        with fs.open(tmp_path, "wb") as out:
            pq.write_table(table, out, compression="snappy")

        return {
            "year": year,
            "month": month,
            "tmp_path": tmp_path,   # sem s3://, formato s3fs
            "rows": table.num_rows,
        }

    @task
    def load_tmp_into_iceberg(item: Dict) -> str:
        year = int(item["year"])
        month = int(item["month"])
        tmp_path = item["tmp_path"]

        start, end = month_range(year, month)

        conn = connect_trino()
        cur = conn.cursor()

        ensure_iceberg_table(cur)

        # idempotência por mês
        cur.execute(
            f"""
            DELETE FROM {ICEBERG_CATALOG}.{ICEBERG_SCHEMA}.{ICEBERG_TABLE}
            WHERE data_da_operacao >= DATE '{start.isoformat()}'
              AND data_da_operacao <  DATE '{end.isoformat()}'
            """
        )

        # lê parquet tmp no Python e insere no Iceberg
        # (sem hive: Trino não lê parquet solto. então inserimos via executemany)
        fs = get_fs()
        with fs.open(tmp_path, "rb") as f:
            t = pq.read_table(f)
        df = t.to_pandas()

        insert_sql = f"""
        INSERT INTO {ICEBERG_CATALOG}.{ICEBERG_SCHEMA}.{ICEBERG_TABLE} (
          codigo_do_investidor,
          data_da_operacao,
          tipo_titulo,
          vencimento_do_titulo,
          quantidade,
          valor_do_titulo,
          valor_da_operacao,
          tipo_da_operacao,
          canal_da_operacao
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """.strip()

        rows = [
            (
                r["codigo_do_investidor"],
                r["data_da_operacao"],
                r["tipo_titulo"],
                r["vencimento_do_titulo"],
                float(r["quantidade"]) if r["quantidade"] is not None else None,
                float(r["valor_do_titulo"]) if r["valor_do_titulo"] is not None else None,
                float(r["valor_da_operacao"]) if r["valor_da_operacao"] is not None else None,
                r["tipo_da_operacao"],
                r["canal_da_operacao"],
            )
            for r in df.to_dict(orient="records")
        ]

        chunk = 2000
        for i in range(0, len(rows), chunk):
            cur.executemany(insert_sql, rows[i : i + chunk])

        cur.close()
        conn.close()
        return f"loaded {len(rows)} rows for {year}-{month:02d}"

    @task
    def cleanup_tmp(item: Dict) -> None:
        fs = get_fs()
        try:
            fs.rm(item["tmp_path"])
        except Exception as e:
            print("cleanup warn:", e)

    files = discover_files()
    transformed = extract_transform_to_tmp.expand(fileinfo=files)
    loaded = load_tmp_into_iceberg.expand(item=transformed)

    # cleanup só depois do load terminar (por map-index)
    cleanup_tmp.expand(item=transformed).set_upstream(loaded)
