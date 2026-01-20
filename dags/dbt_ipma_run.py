from datetime import datetime

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

DBT_IMAGE = "pradovalmur/dbt:1.10.7-trino-1.10.1"

DBT_SRC = "/opt/airflow/dags/repo/dags/dbt/ipma"
DBT_RUN = "/tmp/dbt_ipma"

with DAG(
    dag_id="dbt_ipma_run",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["dbt", "ipma"],
) as dag:

    dbt_debug = KubernetesPodOperator(
        task_id="dbt_debug",
        namespace="orchestration",
        image=DBT_IMAGE,
        name="dbt-ipma-debug",
        cmds=["/bin/bash", "-lc"],
        arguments=[f"""
            set -euo pipefail

            rm -rf "{DBT_RUN}"
            mkdir -p "{DBT_RUN}"

            cp "{DBT_SRC}/dbt_project.yml" "{DBT_RUN}/"
            cp "{DBT_SRC}/profiles.yml" "{DBT_RUN}/"
            cp -R "{DBT_SRC}/models" "{DBT_RUN}/"

            echo "=== DBT_SRC ==="
            ls -la "{DBT_SRC}"
            echo "=== DBT_RUN ==="
            find "{DBT_RUN}" -maxdepth 3 -type f -print

            echo "=== dbt --version ==="
            dbt --version

            echo "=== dbt debug ==="
            dbt debug --profiles-dir "{DBT_RUN}" --project-dir "{DBT_RUN}"
        """],
        get_logs=True,
        is_delete_operator_pod=True,
    )

    dbt_run = KubernetesPodOperator(
        task_id="dbt_run",
        namespace="orchestration",
        image=DBT_IMAGE,
        name="dbt-ipma-run",
        cmds=["/bin/bash", "-lc"],
        arguments=[f"""
            set -euo pipefail

            rm -rf "{DBT_RUN}"
            mkdir -p "{DBT_RUN}"

            cp "{DBT_SRC}/dbt_project.yml" "{DBT_RUN}/"
            cp "{DBT_SRC}/profiles.yml" "{DBT_RUN}/"
            cp -R "{DBT_SRC}/models" "{DBT_RUN}/"

            echo "=== dbt run ==="
            dbt run --profiles-dir "{DBT_RUN}" --project-dir "{DBT_RUN}"
        """],
        get_logs=True,
        is_delete_operator_pod=True,
    )

    dbt_test = KubernetesPodOperator(
        task_id="dbt_test",
        namespace="orchestration",
        image=DBT_IMAGE,
        name="dbt-ipma-test",
        cmds=["/bin/bash", "-lc"],
        arguments=[f"""
            set -euo pipefail

            rm -rf "{DBT_RUN}"
            mkdir -p "{DBT_RUN}"

            cp "{DBT_SRC}/dbt_project.yml" "{DBT_RUN}/"
            cp "{DBT_SRC}/profiles.yml" "{DBT_RUN}/"
            cp -R "{DBT_SRC}/models" "{DBT_RUN}/"

            echo "=== dbt test ==="
            dbt test --profiles-dir "{DBT_RUN}" --project-dir "{DBT_RUN}"
        """],
        get_logs=True,
        is_delete_operator_pod=True,
    )

    dbt_debug >> dbt_run >> dbt_test
