from airflow.sdk import dag, task
from pendulum import datetime
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator


@dag(
    dag_id="orchestrate",
    schedule="0 11 * * *",
    start_date=datetime(2026, 1, 1, tz="America/Sao_Paulo"),
    catchup=False, 
    tags=["walmart"]
)
def orchestrate():

    ingest_cdc = DatabricksRunNowOperator(
        task_id="ingest_cdc",
        databricks_conn_id="databricks",
        job_id="722339675985031",
        deferrable=True,
        )
    
    @task.bash
    def source_freshness():
        return "cd /usr/local/airflow/walmart && dbt source freshness"

    @task.bash
    def run_silver_tech():
        return "cd /usr/local/airflow/walmart && dbt run --select silver_tech"

    @task.bash
    def test_silver_tech():
        return "cd /usr/local/airflow/walmart && dbt test --select silver_tech"

    @task.bash
    def run_silver_biz():
        return "cd /usr/local/airflow/walmart && dbt run --select silver_biz"

    @task.bash
    def test_silver_biz():
        return "cd /usr/local/airflow/walmart && dbt test --select silver_biz"

    @task.bash
    def run_gold_eph():
        return "cd /usr/local/airflow/walmart && dbt run --select gold/ephemeral"

    @task.bash
    def run_gold_dim():
        return "cd /usr/local/airflow/walmart && dbt snapshot"

    @task.bash
    def run_gold_fact():
        return "cd /usr/local/airflow/walmart && dbt run --select gold/fact"


    ingest_cdc() >> source_freshness() >> run_silver_tech() >> test_silver_tech() >> run_silver_biz() >> test_silver_biz() >> run_gold_eph() >> run_gold_dim() >> run_gold_fact()

orchestrate()