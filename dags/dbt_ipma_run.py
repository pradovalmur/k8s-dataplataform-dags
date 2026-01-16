from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

DBT_SRC = "/opt/airflow/dags/repo/dags/dbt/ipma"
DBT_RUN = "/tmp/dbt_ipma"

with DAG(
    dag_id="dbt_ipma_run",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    dbt_debug = BashOperator(
        task_id="dbt_debug",
        bash_command=f"""
        set -euo pipefail

        rm -rf {DBT_RUN}
        mkdir -p {DBT_RUN}

        cp {DBT_SRC}/dbt_project.yml {DBT_RUN}/
        cp {DBT_SRC}/profiles.yml {DBT_RUN}/
        cp -R {DBT_SRC}/models {DBT_RUN}/

        echo "---- dbt_project.yml ----"
        cat {DBT_RUN}/dbt_project.yml
        echo "---- procurando flags no project ----"
        grep -n "flags" {DBT_RUN}/dbt_project.yml || true

        dbt debug --profiles-dir {DBT_RUN} --project-dir {DBT_RUN}
        """,
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"""
        set -euo pipefail
        dbt run --profiles-dir {DBT_RUN} --project-dir {DBT_RUN}
        """,
    )

    dbt_debug >> dbt_run
