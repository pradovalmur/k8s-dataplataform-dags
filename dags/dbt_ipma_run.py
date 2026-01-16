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

    DBT_SRC=/opt/airflow/dags/repo/dags/dbt/ipma
    DBT_RUN=/tmp/dbt_ipma

    rm -rf "$DBT_RUN"
    mkdir -p "$DBT_RUN"

    # copia só o projeto (evita carregar qualquer resíduo)
    cp "$DBT_SRC/dbt_project.yml" "$DBT_RUN/"
    cp "$DBT_SRC/profiles.yml" "$DBT_RUN/"
    cp -R "$DBT_SRC/models" "$DBT_RUN/"

    echo "DBT_RUN content:"
    find "$DBT_RUN" -maxdepth 2 -type f -print

    dbt debug --profiles-dir "$DBT_RUN" --project-dir "$DBT_RUN"
    """,
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"""
        set -euo pipefail
        DBT_RUN=/tmp/dbt_ipma
        dbt run --profiles-dir "$DBT_RUN" --project-dir "$DBT_RUN"
        """,
    )


    dbt_debug >> dbt_run
