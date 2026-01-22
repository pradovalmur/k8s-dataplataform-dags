# dags/ipma_observations_to_iceberg_raw.py
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import requests
from airflow.decorators import dag, task
from airflow.models import Variable

from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.trigger_rule import TriggerRule

from trino import dbapi

IPMA_URL = "https://api.ipma.pt/open-data/observation/meteorology/stations/observations.json"


def _now_utc():
    return datetime.now(timezone.utc)


def _trino_conn():
    return dbapi.connect(
        host=Variable.get("TRINO_HOST"),
        port=int(Variable.get("TRINO_PORT", "8080")),
        user=Variable.get("TRINO_USER", "airflow"),
        http_scheme=Variable.get("TRINO_HTTP_SCHEME", "http"),
        catalog=Variable.get("TRINO_CATALOG", "iceberg"),
        schema=Variable.get("TRINO_SCHEMA", "raw"),
    )


def _sql_escape_literal(s: str) -> str:
    return s.replace("'", "''")


@dag(
    dag_id="ipma_observations",
    schedule="15 * * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["ipma", "observations", "iceberg"],
)
def ipma_observations():
    @task
    def check_api() -> int:
        r = requests.get(IPMA_URL, timeout=20)
        r.raise_for_status()
        return r.status_code

    @task
    def fetch(_: int) -> dict:
        r = requests.get(IPMA_URL, timeout=60)
        r.raise_for_status()
        payload = r.json()
        if not isinstance(payload, dict):
            raise ValueError(f"observations.json: esperado dict, veio {type(payload)}")
        return payload

    @task
    def insert(payload: dict) -> dict:
        now = _now_utc()
        ingested_at = now.replace(tzinfo=None).isoformat(sep=" ", timespec="microseconds")
        dt = now.strftime("%Y-%m-%d")
        hour = now.strftime("%H")

        payload_str = json.dumps(payload, ensure_ascii=False)
        payload_sql = _sql_escape_literal(payload_str)

        sql = f"""
        INSERT INTO ipma_observations_raw (ingested_at, dt, hour, payload)
        VALUES (TIMESTAMP '{ingested_at}', DATE '{dt}', '{hour}', '{payload_sql}')
        """

        conn = _trino_conn()
        cur = conn.cursor()
        cur.execute(sql)

        return {"dt": dt, "hour": hour, "bytes": len(payload_str)}

    trigger_dbt_ipma_run = TriggerDagRunOperator(
        task_id="trigger_dbt_ipma_run",
        trigger_dag_id="dbt_ipma_run",
        wait_for_completion=False,
        reset_dag_run=True,
        trigger_rule=TriggerRule.ALL_SUCCESS,  # só dispara se insert terminar OK
    )

    status = check_api()
    payload = fetch(status)
    inserted = insert(payload)

    inserted >> trigger_dbt_ipma_run


ipma_observations()
