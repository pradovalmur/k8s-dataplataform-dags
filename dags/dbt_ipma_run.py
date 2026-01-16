from datetime import datetime
import os

from airflow import DAG
from airflow.operators.bash import BashOperator

# resolve o symlink criado pelo git-sync
DAGS_REPO = os.path.realpath("/opt/airflow/dags/repo")

# dbt está em dags/dbt/ipma
DBT_DIR = os.path.join(DAGS_REPO, "dags", "dbt", "ipma")

with DAG(
    dag_id="dbt_ipma_run",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    dbt_debug = BashOperator(
        task_id="dbt_debug",
        bash_command=f"""
        set -e
        echo "DBT_DIR={DBT_DIR}"
        ls -la {DBT_DIR}
        cd {DBT_DIR}
        dbt debug --profiles-dir {DBT_DIR}
        """,
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"""
        set -e
        cd {DBT_DIR}
        dbt run --profiles-dir {DBT_DIR}
        """,
    )

    dbt_debug >> dbt_run
