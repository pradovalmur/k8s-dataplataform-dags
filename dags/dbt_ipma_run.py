from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s

NAMESPACE = "orchestration"
SERVICE_ACCOUNT = "airflow"

DBT_IMAGE = "pradovalmur/dbt:1.10.7-trino-1.10.1"

# PVC que contém /opt/airflow/dags + git-sync output
DAGS_PVC_NAME = "airflow-dags"

# Dentro do pod efêmero, vamos montar o PVC aqui:
DAGS_MOUNT_PATH = "/opt/airflow/dags"

# dbt project fica dentro do repo: dags/dbt/ipma
DBT_PROJECT_REL = "dags/dbt/ipma"

# área de execução (cópia local)
DBT_RUN = "/tmp/dbt_ipma"

dags_volume = k8s.V1Volume(
    name="dags",
    persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name=DAGS_PVC_NAME),
)

dags_mount = k8s.V1VolumeMount(
    name="dags",
    mount_path=DAGS_MOUNT_PATH,
    read_only=True,
)

def bash_cmd(dbt_cmd: str) -> str:
    return f"""
set -euo pipefail

echo "=== locate worktree ==="
WT="$(ls -1 {DAGS_MOUNT_PATH}/.worktrees 2>/dev/null | head -n 1 || true)"
if [ -z "$WT" ]; then
  echo "ERROR: no worktree found at {DAGS_MOUNT_PATH}/.worktrees"
  ls -la {DAGS_MOUNT_PATH} || true
  exit 2
fi

REPO="{DAGS_MOUNT_PATH}/.worktrees/$WT"
DBT_SRC="$REPO/{DBT_PROJECT_REL}"

echo "=== repo check ==="
ls -la "$REPO" || true
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
    volumes=[dags_volume],
    volume_mounts=[dags_mount],
    service_account_name=SERVICE_ACCOUNT,
    in_cluster=True,
    is_delete_operator_pod=True,
    get_logs=True,
    do_xcom_push=False,
)

MODELS = [
    "stg_ipma_observations",
]

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
                # args precisa ser string (não lista)
                arguments=bash_cmd(f"run --select {model}"),
                **BASE_KPO,
            )

            dbt_test = KubernetesPodOperator(
                task_id=f"test__{model}",
                name=f"dbt-test-{model}".replace("_", "-"),
                arguments=bash_cmd(f"test --select {model}"),
                **BASE_KPO,
            )

            dbt_run >> dbt_test

    dbt_models
