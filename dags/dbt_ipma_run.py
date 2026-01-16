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
    tags=["dbt", "ipma"],
) as dag:

    dbt_debug = BashOperator(
        task_id="dbt_debug",
        bash_command=f"""
        set -euo pipefail

        rm -rf "{DBT_RUN}"
        mkdir -p "{DBT_RUN}"

        # Copia só o que interessa (evita lixo de runs anteriores)
        cp "{DBT_SRC}/dbt_project.yml" "{DBT_RUN}/"
        cp "{DBT_SRC}/profiles.yml" "{DBT_RUN}/"
        cp -R "{DBT_SRC}/models" "{DBT_RUN}/"

        echo "=== DBT_SRC ==="
        ls -la "{DBT_SRC}"
        echo "=== DBT_RUN ==="
        find "{DBT_RUN}" -maxdepth 3 -type f -print

        echo "=== which dbt ==="
        which dbt || true
        ls -la "$(which dbt)" || true

        echo "=== head do binario dbt (se for script) ==="
        head -n 80 "$(which dbt)" || true

        echo "=== env (DBT/TRINO/SSL/PROXY) ==="
        env | sort | egrep -i "^(DBT|TRINO|REQUESTS_CA_BUNDLE|CURL_CA_BUNDLE|SSL_CERT_FILE|SSL_CERT_DIR|HTTPS?_PROXY|NO_PROXY)=" || true

        echo "=== dbt_project.yml ==="
        cat "{DBT_RUN}/dbt_project.yml"

        echo "=== profiles.yml ==="
        cat "{DBT_RUN}/profiles.yml"

        # Se algum wrapper/env estiver injetando flags globais, zera aqui
        unset DBT_GLOBAL_FLAGS DBT_FLAGS || true

        echo "=== dbt --version ==="
        dbt --version

        echo "=== dbt debug ==="
        dbt debug --profiles-dir "{DBT_RUN}" --project-dir "{DBT_RUN}"
        """,
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"""
        set -euo pipefail

        unset DBT_GLOBAL_FLAGS DBT_FLAGS || true

        echo "=== dbt run ==="
        dbt run --profiles-dir "{DBT_RUN}" --project-dir "{DBT_RUN}"
        """,
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"""
        set -euo pipefail

        unset DBT_GLOBAL_FLAGS DBT_FLAGS || true

        echo "=== dbt test ==="
        dbt test --profiles-dir "{DBT_RUN}" --project-dir "{DBT_RUN}"
        """,
    )

    dbt_debug >> dbt_run >> dbt_test
