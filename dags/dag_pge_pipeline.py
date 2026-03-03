from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG
from yaml import load, SafeLoader
from pathlib import Path


UV = "/home/ec2-user/.local/bin/uv"
PGE_PROJECT = "/home/ec2-user/apps/pge_etl"
DBT_PROJECT = "/home/ec2-user/apps/kwb_dw"

config_path = Path(PGE_PROJECT) / "config" / "sources.yaml"
with open(config_path) as f:
    config = load(f, SafeLoader)

dag_id = "pge_etl"
schedule = "0 +/4 * * *"
with DAG(dag_id, schedule=schedule, catchup=False) as dag:
    source_tasks = []
    all_dbt_models = []

    for source_cfg in config["sources"]:
        task = BashOperator(
            task_id=source_cfg["name"],
            bash_command=f"cd {PGE_PROJECT} && {UV} run python -m src.main",
        )
        source_tasks.append(task)

        if source_cfg.get("dbt_models"):
            all_dbt_models.append(source_cfg["dbt_models"])

    if all_dbt_models:
        select_arg = " ".join(all_dbt_models)
        dbt_task = BashOperator(
            task_id="dbt_build",
            bash_command=f"cd {DBT_PROJECT} && {UV} run dbt build --target prod --select {select_arg}",
        )
        source_tasks >> dbt_task

    globals()[dag_id] = dag
