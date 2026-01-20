from datetime import datetime

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s

NAMESPACE = "orchestration"

REPO_URL = "https://github.com/pradovalmur/k8s-dataplataform-dags.git"
REPO_BRANCH = "main"

WORKDIR = "/workspace"
REPO_DIR = f"{WORKDIR}/repo"

DBT_SRC = f"{REPO_DIR}/dags/dbt/ipma"
DBT_RUN = "/tmp/dbt_ipma"

# Volume efêmero (por Pod do KPO)
repo_vol = k8s.V1Volume(
    name="repo",
    empty_dir=k8s.V1EmptyDirVolumeSource(),
)
repo_mount = k8s.V1VolumeMount(
    name="repo",
    mount_path=WORKDIR,
)

# Init container: faz clone do repo (mais confiável que git-sync aqui)
git_clone_init = k8s.V1Container(
    name="git-clone",
    image="alpine/git:2.45.2",
    image_pull_policy="IfNotPresent",
    command=["/bin/sh", "-lc"],
    args=[
        f"""
        set -euo pipefail
        rm -rf "{REPO_DIR}"
        mkdir -p "{WORKDIR}"

        echo "Clonando repo..."
        git clone --depth 1 --branch "{REPO_BRANCH}" "{REPO_URL}" "{REPO_DIR}"

        echo "Repo clonado em: {REPO_DIR}"
        ls -la "{REPO_DIR}" | head -n 50
        """.strip()
    ],
    volume_mounts=[repo_mount],
)

def make_cmd(subcommand: str) -> str:
    # roda no container principal (imagem do dbt)
    return f"""
set -euo pipefail

echo "=== sanity ==="
id
ls -la "{WORKDIR}" || true
ls -la "{REPO_DIR}" || true
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
        volumes=[repo_vol],
        volume_mounts=[repo_mount],
        init_containers=[git_clone_init],
        service_account_name="airflow",
        in_cluster=True,
        get_logs=True,
        is_delete_operator_pod=True,
        do_xcom_push=False,
        startup_timeout_seconds=600,
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
