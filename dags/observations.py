# dags/ipma_observations_raw_to_minio_parquet.py
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pandas as pd
import requests
import s3fs

from airflow.decorators import dag, task
from airflow.models import Variable

IPMA_URL = "https://api.ipma.pt/open-data/observation/meteorology/stations/observations.json"

def _now_utc():
    return datetime.now(timezone.utc)

def _s3_fs() -> s3fs.S3FileSystem:
    return s3fs.S3FileSystem(
        key=Variable.get("MINIO_ACCESS_KEY"),
        secret=Variable.get("MINIO_SECRET_KEY"),
        client_kwargs={
            "endpoint_url": Variable.get("MINIO_ENDPOINT"),
        },
    )

@dag(
    dag_id="ipma_observations_raw_to_minio_parquet",
    schedule="15 * * * *",  # a cada hora no minuto 15
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["ipma", "raw", "minio", "observations", "parquet"],
)
def ipma_observations_raw_to_minio_parquet():
    @task
    def fetch_and_write():
        bucket = Variable.get("RAW_BUCKET")
        prefix = Variable.get("RAW_PREFIX", "raw/ipma")

        resp = requests.get(IPMA_URL, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        if not isinstance(data, list):
            raise ValueError("observations.json: esperado array JSON")

        df = pd.json_normalize(data)

        now = _now_utc()
        df["ingested_at"] = now.isoformat()
        df["dt"] = now.strftime("%Y-%m-%d")
        df["hour"] = now.strftime("%H")

        dt = now.strftime("%Y-%m-%d")
        hour = now.strftime("%H")
        ts = now.strftime("%H%M%S")

        path = f"{bucket}/{prefix}/observations/dt={dt}/hour={hour}/observations_{ts}.parquet"

        fs = _s3_fs()
        with fs.open(path, "wb") as f:
            df.to_parquet(f, index=False, engine="pyarrow")

        return {"rows": int(len(df)), "path": f"s3://{path}"}

    fetch_and_write()

ipma_observations_raw_to_minio_parquet()
