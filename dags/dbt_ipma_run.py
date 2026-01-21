from datetime import datetime

from airflow import DAG
from airflow.utils.task_group import TaskGroup

from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from cosmos.constants import LoadMode
from cosmos.profiles import ProfileMapping

NAMESPACE = "orchestration"

# caminho do repo já montado no pod (o mesmo que funcionou pra você)
DBT_PROJECT_PATH = "/opt/airflow/dags/repo/dags/dbt/ipma"
DBT_PROFILES_PATH = DBT_PROJECT_PATH  # profiles.yml dentro do mesmo diretório

DBT_IMAGE = "pradovalmur/dbt:1.10.7-trino-1.10.1"

# Ajuste pra sua config do profiles.yml
# Opção 1 (mais simples): Cosmos lê o profiles.yml do volume
profile_config = ProfileConfig(
    profile_name="ipma",         # nome do profile dentro do profiles.yml
    target_name="dev",           # target dentro do profiles.yml
    profiles_yml_filepath=f"{DBT_PROFILES_PATH}/profiles.yml",
)

execution_config = ExecutionConfig(
    execution_mode="kubernetes",
    kubernetes_namespace=NAMESPACE,
    kubernetes_service_account_name="airflow",
    kubernetes_image=DBT_IMAGE,
    # se precisar setar env no pod (TRINO_PASSWORD etc), dá pra adicionar aqui
    # env_vars={"TRINO_PASSWORD": "..."},
)

project_config = ProjectConfig(
    dbt_project_path=DBT_PROJECT_PATH,
)

# aqui você controla como o Cosmos “descobre” o DAG dbt
render_config = RenderConfig(
    load_method=LoadMode.DBT_LS,  # usa `dbt ls` pra listar nodes
    # seleção: só alguns modelos (recomendado pra começar)
    select=["stg_ipma_observations"],
    # para todos os models: tira o select e usa o dbt graph inteiro
    # select=["path:models"],
)


with DAG(
    dag_id="dbt_ipma_cosmos",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["dbt", "ipma", "cosmos"],
) as dag:

    # taskgroup de “run”
    tg_run = DbtTaskGroup(
        group_id="dbt_run",
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=render_config,
        operator_args={
            # esses args vão pro KubernetesPodOperator que o Cosmos cria por baixo
            "get_logs": True,
            "is_delete_operator_pod": True,
            "image_pull_policy": "Always",
        },
        # por padrão ele roda dbt run nos nodes selecionados
    )

    # taskgroup de “test” (mesma seleção, mas comando test)
    tg_test = DbtTaskGroup(
        group_id="dbt_test",
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=render_config,
        operator_args={
            "get_logs": True,
            "is_delete_operator_pod": True,
            "image_pull_policy": "Always",
        },
        dbt_command="test",
    )

    tg_run >> tg_test
