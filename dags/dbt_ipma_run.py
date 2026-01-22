from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s


NAMESPACE = "orchestration"
SERVICE_ACCOUNT = "airflow"
DBT_IMAGE = "pradovalmur/dbt:1.10.7-trino-1.10.1"

GIT_REPO = "https://github.com/pradovalmur/k8s-dataplataform-dags.git"
GIT_BRANCH = "main"

DBT_SRC = "/tmp/repo/dags/dbt/ipma"
DBT_RUN = "/tmp/dbt_ipma"


def bash_cmd(dbt_cmd: str) -> str:
    return f"""
set -euo pipefail

apk add --no-cache git >/dev/null 2>&1 || true

rm -rf /tmp/repo
git clone --depth 1 --branch "{GIT_BRANCH}" "{GIT_REPO}" /tmp/repo

echo "=== repo check ==="
ls -la "{DBT_SRC}" || true
test -f "{DBT_SRC}/dbt_project.yml"
test -f "{DBT_SRC}/profiles.yml"

rm -rf "{DBT_RUN}"
mkdir -p "{DBT_RUN}"

cp "{DBT_SRC}/dbt_project.yml" "{DBT_RUN}/"
cp "{DBT_SRC}/profiles.yml" "{DBT_RUN}/"
cp -R "{DBT_SRC}/models" "{DBT_RUN}/"
cp -R "{DBT_SRC}/macros" "{DBT_RUN}/"

unset DBT_GLOBAL_FLAGS DBT_FLAGS || true

echo "=== dbt {dbt_cmd} ==="
dbt {dbt_cmd} --profiles-dir "{DBT_RUN}" --project-dir "{DBT_RUN}"
""".strip()


BASE_KPO = dict(
    namespace=NAMESPACE,
    image=DBT_IMAGE,
    image_pull_policy="Always",
    cmds=["/bin/bash", "-lc"],
    service_account_name=SERVICE_ACCOUNT,
    in_cluster=True,
    is_delete_operator_pod=True,
    get_logs=True,
    do_xcom_push=False,
)

STG_MODELS = [
    "stg_ipma_observations",
    "stg_ipma_stations",
]

DELIVERY_MODELS = [
    "dim_station",
    "fact_observation_hourly",
    "fact_observation_enriched",
    "fact_station_daily",
    "fact_station_latest",
]

with DAG(
    dag_id="dbt_ipma_run",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["dbt", "ipma"],
) as dag:

    with TaskGroup(group_id="stg") as tg_stg:
        for m in STG_MODELS:
            run = KubernetesPodOperator(
                task_id=f"run__{m}",
                name=f"dbt-run-{m}".replace("_", "-"),
                arguments=[bash_cmd(f"run --select {m}")],
                **BASE_KPO,
            )
            test = KubernetesPodOperator(
                task_id=f"test__{m}",
                name=f"dbt-test-{m}".replace("_", "-"),
                arguments=[bash_cmd(f"test --select {m}")],
                **BASE_KPO,
            )
            run >> test

    with TaskGroup(group_id="delivery") as tg_delivery:
        prev = None
        for m in DELIVERY_MODELS:
            run = KubernetesPodOperator(
                task_id=f"run__{m}",
                name=f"dbt-run-{m}".replace("_", "-"),
                arguments=[bash_cmd(f"run --select {m}")],
                **BASE_KPO,
            )
            test = KubernetesPodOperator(
                task_id=f"test__{m}",
                name=f"dbt-test-{m}".replace("_", "-"),
                arguments=[bash_cmd(f"test --select {m}")],
                **BASE_KPO,
            )

            run >> test

            # garante sequência: (run+test) do modelo anterior termina antes do próximo começar
            if prev is not None:
                prev >> run
            prev = test

    tg_stg >> tg_delivery
