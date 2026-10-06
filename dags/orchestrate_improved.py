from airflow.sdk import dag, chain
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from pendulum import datetime


def dbt(task_id: str, command: str) -> BashOperator:
    # DBT_PROJECT_DIR and DBT_PROFILES_DIR come from .env, so no `cd` is needed
    return BashOperator(task_id=task_id, bash_command=f"dbt {command}")

@dag(
    dag_id="orchestrate_improved",
    schedule="0 11 * * *",
    start_date=datetime(2026, 1, 1, tz="America/Sao_Paulo"),
    catchup=False,
    tags=["walmart"],
)
def orchestrate():

    ingest_cdc = DatabricksRunNowOperator(
        task_id="ingest_cdc",
        databricks_conn_id="databricks",
        job_id="722339675985031",
        deferrable=True,
    )

    chain(
        ingest_cdc,
        dbt("source_freshness", "source freshness"),
        dbt("run_silver_tech", "run --select silver_tech"),
        dbt("test_silver_tech", "test --select silver_tech"),
        dbt("run_silver_biz", "run --select silver_biz"),
        dbt("test_silver_biz", "test --select silver_biz"),
        dbt("run_gold_eph", "run --select gold/ephemeral"),
        dbt("run_gold_dim", "snapshot"),
        dbt("run_gold_fact", "run --select gold/fact"),
    )

orchestrate()