from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

# caminho "lógico" via symlink
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

        echo "DBT_SRC={DBT_SRC}"
        ls -la /opt/airflow/dags || true
        ls -la /opt/airflow/dags/repo || true
        ls -la {DBT_SRC}

        rm -rf {DBT_RUN}
        mkdir -p {DBT_RUN}
        cp -R {DBT_SRC}/. {DBT_RUN}/

        echo "DBT_RUN={DBT_RUN}"
        ls -la {DBT_RUN}

        cd {DBT_RUN}
        dbt debug --profiles-dir {DBT_RUN}
        """,
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"""
        set -euo pipefail
        cd {DBT_RUN}
        dbt run --profiles-dir {DBT_RUN}
        """,
    )

    dbt_debug >> dbt_run
