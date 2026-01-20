from datetime import datetime

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s

NAMESPACE = "orchestration"

REPO_URL = "https://github.com/pradovalmur/k8s-dataplataform-dags.git"
REPO_BRANCH = "main"

GIT_ROOT = "/workspace"
GIT_DEST = "repo"

DBT_SRC = f"{GIT_ROOT}/{GIT_DEST}/dags/dbt/ipma"
DBT_RUN = "/tmp/dbt_ipma"

# Shared volume for git-sync -> dbt container
shared_repo = k8s.V1Volume(
    name="repo",
    empty_dir=k8s.V1EmptyDirVolumeSource(),
)
shared_repo_mount = k8s.V1VolumeMount(
    name="repo",
    mount_path=GIT_ROOT,
)

git_sync_init = k8s.V1Container(
    name="git-sync-init",
    image="registry.k8s.io/git-sync/git-sync:v4.2.3",
    image_pull_policy="IfNotPresent",
    env=[
        k8s.V1EnvVar(name="GIT_SYNC_REPO", value=REPO_URL),
        k8s.V1EnvVar(name="GIT_SYNC_BRANCH", value=REPO_BRANCH),
        k8s.V1EnvVar(name="GIT_SYNC_ROOT", value=GIT_ROOT),
        k8s.V1EnvVar(name="GIT_SYNC_DEST", value=GIT_DEST),
        k8s.V1EnvVar(name="GIT_SYNC_ONE_TIME", value="true"),
        # evita ssh known_hosts etc (repo https)
        k8s.V1EnvVar(name="GIT_SYNC_DEPTH", value="1"),
        k8s.V1EnvVar(name="GIT_SYNC_MAX_FAILURES", value="-1"),
    ],
    volume_mounts=[shared_repo_mount],
)

def make_cmd(subcommand: str) -> str:
    # subcommand: debug / run / test
    return f"""
set -euo pipefail

echo "=== repo tree ==="
ls -la "{GIT_ROOT}" || true
ls -la "{GIT_ROOT}/{GIT_DEST}" || true
ls -la "{DBT_SRC}" || true

test -f "{DBT_SRC}/dbt_project.yml"
test -f "{DBT_SRC}/profiles.yml"

rm -rf "{DBT_RUN}"
mkdir -p "{DBT_RUN}"

cp "{DBT_SRC}/dbt_project.yml" "{DBT_RUN}/"
cp "{DBT_SRC}/profiles.yml" "{DBT_RUN}/"
cp -R "{DBT_SRC}/models" "{DBT_RUN}/"

unset DBT_GLOBAL_FLAGS DBT_FLAGS || true

echo "=== dbt --version ==="
dbt --version

echo "=== dbt {subcommand} ==="
dbt {subcommand} --profiles-dir "{DBT_RUN}" --project-dir "{DBT_RUN}"
""".strip()

with DAG(
    dag_id="dbt_ipma_run",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["dbt", "ipma"],
) as dag:
    base_kwargs = dict(
        namespace=NAMESPACE,
        image="pradovalmur/dbt:1.10.7-trino-1.10.1",
        image_pull_policy="Always",
        cmds=["/bin/bash", "-lc"],
        volumes=[shared_repo],
        volume_mounts=[shared_repo_mount],
        init_containers=[git_sync_init],
        service_account_name="airflow",
        in_cluster=True,
        get_logs=True,
        is_delete_operator_pod=True,
        do_xcom_push=False,
        # evita “Pod not yet started” virar fail cedo
        startup_timeout_seconds=600,
        # dá tempo pro dbt em clusters pequenos
        execution_timeout=None,
        # padroniza labels
        labels={"app": "airflow-kpo", "component": "dbt"},
    )

    dbt_debug = KubernetesPodOperator(
        task_id="dbt_debug",
        name="dbt-ipma-debug",
        arguments=[make_cmd("debug")],
        **base_kwargs,
    )

    dbt_run = KubernetesPodOperator(
        task_id="dbt_run",
        name="dbt-ipma-run",
        arguments=[make_cmd("run")],
        **base_kwargs,
    )

    dbt_test = KubernetesPodOperator(
        task_id="dbt_test",
        name="dbt-ipma-test",
        arguments=[make_cmd("test")],
        **base_kwargs,
    )

    dbt_debug >> dbt_run >> dbt_test
