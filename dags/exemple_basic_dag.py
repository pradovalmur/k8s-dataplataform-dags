from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

with DAG(
    dag_id="example_basic_dag",
    default_args=default_args,
    description="DAG simples para testar o Airflow",
    schedule_interval="@hourly",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["example"],
) as dag:

    task_hello = BashOperator(
        task_id="hello",
        bash_command='echo "Hello Airflow from Kubernetes!"'
    )

    task_date = BashOperator(
        task_id="show_date",
        bash_command="date"
    )

    task_hello >> task_date
