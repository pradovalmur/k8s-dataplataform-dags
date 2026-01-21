from __future__ import annotations

from datetime import datetime
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

NAMESPACE = "orchestration"
SERVICE_ACCOUNT = "airflow"

DBT_IMAGE = "pradovalmur/dbt:1.10.7-trino-1.10.1"

REPO_URL = "https://github.com/pradovalmur/k8s-dataplataform-dags.git"
REPO_BRANCH = "main"

DBT_PROJECT_REL = "dags/dbt/ipma"
DBT_RUN = "/tmp/dbt_ipma"

def bash_cmd(dbt_cmd: str) -> str:
    return f"""
set -euo pipefail

# instala git (se a imagem já tiver, isso não quebra)
apk add --no-cache git >/dev/null 2>&1 || true

rm -rf /tmp/repo
git clone --depth 1 --branch "{REPO_BRANCH}" "{REPO_URL}" /tmp/repo

DBT_SRC="/tmp/repo/{DBT_PROJECT_REL}"

echo "=== repo check ==="
ls -la "$DBT_SRC" || true
test -f "$DBT_SRC/dbt_project.yml"
test -f "$DBT_SRC/profiles.yml"

rm -rf "{DBT_RUN}"
mkdir -p "{DBT_RUN}"

cp "$DBT_SRC/dbt_project.yml" "{DBT_RUN}/"
cp "$DBT_SRC/profiles.yml" "{DBT_RUN}/"
cp -R "$DBT_SRC/models" "{DBT_RUN}/"

unset DBT_GLOBAL_FLAGS DBT_FLAGS || true

echo "=== dbt --version ==="
dbt --version

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
    startup_timeout_seconds=600,
)

MODELS = ["stg_ipma_observations"]

with DAG(
    dag_id="dbt_ipma_run",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["dbt", "ipma"],
) as dag:

    with TaskGroup(group_id="dbt_models") as dbt_models:
        for model in MODELS:
            dbt_run = KubernetesPodOperator(
                task_id=f"run__{model}",
                name=f"dbt-run-{model}".replace("_", "-"),
                arguments=[bash_cmd(f"run --select {model}")],  # <-- LISTA
                **BASE_KPO,
            )

            dbt_test = KubernetesPodOperator(
                task_id=f"test__{model}",
                name=f"dbt-test-{model}".replace("_", "-"),
                arguments=[bash_cmd(f"test --select {model}")],  # <-- LISTA
                **BASE_KPO,
            )

            dbt_run >> dbt_test

    dbt_models
