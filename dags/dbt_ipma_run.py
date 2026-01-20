from datetime import datetime
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s

NAMESPACE = "orchestration"

REPO_URL = "https://github.com/pradovalmur/k8s-dataplataform-dags.git"
REPO_BRANCH = "main"

# onde o repo vai ficar dentro do pod do dbt
GIT_ROOT = "/workspace"
GIT_DEST = "repo"

DBT_SRC = f"{GIT_ROOT}/{GIT_DEST}/dags/dbt/ipma"
DBT_RUN = "/tmp/dbt_ipma"

def make_dbt_cmd(cmd: str) -> str:
    return f"""
set -euo pipefail

echo "=== wait repo ==="
# espera git-sync popular o repo
for i in $(seq 1 60); do
  if [ -f "{DBT_SRC}/dbt_project.yml" ]; then
    break
  fi
  echo "waiting repo... ($i)"
  sleep 2
done

ls -la "{DBT_SRC}" || true

rm -rf "{DBT_RUN}"
mkdir -p "{DBT_RUN}"

cp "{DBT_SRC}/dbt_project.yml" "{DBT_RUN}/"
cp "{DBT_SRC}/profiles.yml" "{DBT_RUN}/"
cp -R "{DBT_SRC}/models" "{DBT_RUN}/"

echo "=== DBT_RUN ==="
find "{DBT_RUN}" -maxdepth 3 -type f -print

unset DBT_GLOBAL_FLAGS DBT_FLAGS || true

echo "=== dbt --version ==="
dbt --version

echo "=== dbt {cmd} ==="
dbt {cmd} --profiles-dir "{DBT_RUN}" --project-dir "{DBT_RUN}"
""".strip()

with DAG(
    dag_id="dbt_ipma_run",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["dbt", "ipma"],
) as dag:

    shared_repo = k8s.V1Volume(
        name="repo",
        empty_dir=k8s.V1EmptyDirVolumeSource(),
    )
    shared_repo_mount = k8s.V1VolumeMount(
        name="repo",
        mount_path=GIT_ROOT,
    )

    git_sync = k8s.V1Container(
        name="git-sync",
        image="registry.k8s.io/git-sync/git-sync:v4.2.3",
        image_pull_policy="IfNotPresent",
        env=[
            k8s.V1EnvVar(name="GIT_SYNC_REPO", value=REPO_URL),
            k8s.V1EnvVar(name="GIT_SYNC_BRANCH", value=REPO_BRANCH),
            k8s.V1EnvVar(name="GIT_SYNC_ROOT", value=GIT_ROOT),
            k8s.V1EnvVar(name="GIT_SYNC_DEST", value=GIT_DEST),
            k8s.V1EnvVar(name="GIT_SYNC_WAIT", value="30"),
        ],
        volume_mounts=[shared_repo_mount],
    )

    dbt_debug = KubernetesPodOperator(
        task_id="dbt_debug",
        namespace=NAMESPACE,
        name="dbt-ipma-debug",
        image="pradovalmur/dbt:1.10.7-trino-1.10.1",
        image_pull_policy="Always",
        cmds=["/bin/bash", "-lc"],
        arguments=[make_dbt_cmd("debug")],
        volumes=[shared_repo],
        volume_mounts=[shared_repo_mount],
        service_account_name="airflow",
        is_delete_operator_pod=True,
        get_logs=True,
        in_cluster=True,
        do_xcom_push=False,
        additional_containers=[git_sync],
    )

    dbt_run = KubernetesPodOperator(
        task_id="dbt_run",
        namespace=NAMESPACE,
        name="dbt-ipma-run",
        image="pradovalmur/dbt:1.10.7-trino-1.10.1",
        image_pull_policy="Always",
        cmds=["/bin/bash", "-lc"],
        arguments=[make_dbt_cmd("run")],
        volumes=[shared_repo],
        volume_mounts=[shared_repo_mount],
        service_account_name="airflow",
        is_delete_operator_pod=True,
        get_logs=True,
        in_cluster=True,
        do_xcom_push=False,
        additional_containers=[git_sync],
    )

    dbt_test = KubernetesPodOperator(
        task_id="dbt_test",
        namespace=NAMESPACE,
        name="dbt-ipma-test",
        image="pradovalmur/dbt:1.10.7-trino-1.10.1",
        image_pull_policy="Always",
        cmds=["/bin/bash", "-lc"],
        arguments=[make_dbt_cmd("test")],
        volumes=[shared_repo],
        volume_mounts=[shared_repo_mount],
        service_account_name="airflow",
        is_delete_operator_pod=True,
        get_logs=True,
        in_cluster=True,
        do_xcom_push=False,
        additional_containers=[git_sync],
    )

    dbt_debug >> dbt_run >> dbt_test
