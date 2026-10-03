"""Upload the Walmart CSVs to GCS (raw/), then create and load the BigQuery tables."""
from airflow.sdk import dag
from pendulum import datetime
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator

INCLUDE_DIR = "/usr/local/airflow/include"
LOCAL_CSV = f"{INCLUDE_DIR}/walmart_dataset/data/*.csv"
BUCKET_NAME = "walmart-dataset1"
GCP_CONN_ID = "gcp"


@dag(
    dag_id="csv_to_bigquery",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    description="Upload CSVs to GCS, then create and load BigQuery tables",
    tags=["walmart"],
    doc_md=__doc__,
    template_searchpath=[INCLUDE_DIR],
)
def csv_to_bigquery():

    # Idempotent: logs and continues if the bucket already exists
    create_bucket = GCSCreateBucketOperator(
        task_id="create_bucket",
        bucket_name=BUCKET_NAME,
        gcp_conn_id=GCP_CONN_ID,
        storage_class='REGIONAL',
        location='us-east1' # For free tier use
    )

    # Wildcard src + dst ending in "/" => every CSV lands in gs://<bucket>/raw/<name>.csv
    upload_csvs_to_gcs = LocalFilesystemToGCSOperator(
        task_id="upload_csvs_to_gcs",
        src=LOCAL_CSV,
        dst="raw/",
        bucket=BUCKET_NAME,
        mime_type="text/csv",
        gcp_conn_id=GCP_CONN_ID,
    )

    load_tables = BigQueryInsertJobOperator(
        task_id="create_and_load_tables",
        configuration={
            "query": {
                "query": "{% include 'walmart_dataset/ddl/walmart_schema.sql' %}",
                "useLegacySql": False,
            }
        },
        gcp_conn_id=GCP_CONN_ID,
    )

    create_bucket >> upload_csvs_to_gcs >> load_tables

csv_to_bigquery()