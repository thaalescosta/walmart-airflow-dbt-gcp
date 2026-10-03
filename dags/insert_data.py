"""Upload the Walmart CSVs to GCS (raw/), create the BigQuery tables, then load each CSV."""
from airflow.sdk import dag
from pendulum import datetime
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator

INCLUDE_DIR = "/usr/local/airflow/include"
LOCAL_CSV = f"{INCLUDE_DIR}/walmart_dataset/data/*.csv"
BUCKET_NAME = "walmart-dataset1"
BUCKET_LOCATION = "us-east1"   # free-tier region for the bucket
BQ_LOCATION = "US"             # BigQuery dataset/job location (US multi-region can load from any bucket)
BQ_DATASET = "walmart_db"
GCP_CONN_ID = "gcp"

TABLES = ["customers", "stores", "products", "employees", "orders", "order_items"]


# ---- Explicit load schemas (must mirror walmart_schema.sql) ----
# GCSToBigQueryOperator needs a schema when autodetect=False, and WRITE_TRUNCATE
# applies the load schema to the table, so STRING lengths and NUMERIC precision are included.
def _col(name, bq_type, **extra):
    return {"name": name, "type": bq_type, "mode": "NULLABLE", **extra}


def _int(name):
    return _col(name, "INT64")


def _str(name, length):
    return _col(name, "STRING", maxLength=str(length))


def _num(name, precision, scale):
    return _col(name, "NUMERIC", precision=str(precision), scale=str(scale))


def _ts(name):
    return _col(name, "TIMESTAMP")


_AUDIT = [_ts("created_timestamp"), _ts("updated_timestamp"), _str("is_active", 1)]

SCHEMAS = {
    "customers": [
        _int("customer_id"), _str("first_name", 100), _str("last_name", 100),
        _str("email", 255), _str("phone", 50), _str("city", 100),
        _str("province", 100), _str("country", 100), *_AUDIT,
    ],
    "stores": [
        _int("store_id"), _str("store_name", 255), _str("city", 100),
        _str("province", 100), _str("country", 100), *_AUDIT,
    ],
    "products": [
        _int("product_id"), _str("product_name", 255), _str("category", 100),
        _str("brand", 100), _num("price", 10, 2), *_AUDIT,
    ],
    "employees": [
        _int("employee_id"), _int("store_id"), _str("first_name", 100),
        _str("last_name", 100), _str("email", 255), _str("job_title", 100),
        _num("salary", 10, 2), *_AUDIT,
    ],
    "orders": [
        _int("order_id"), _int("customer_id"), _int("store_id"),
        _ts("order_timestamp"), _str("payment_method", 50), _str("order_status", 50),
        _num("total_amount", 12, 2), *_AUDIT,
    ],
    "order_items": [
        _int("order_item_id"), _int("order_id"), _int("product_id"),
        _int("quantity"), _num("unit_price", 10, 2), _num("line_amount", 12, 2), *_AUDIT,
    ],
}


@dag(
    dag_id="csv_to_bigquery",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    description="Upload CSVs to GCS, create BigQuery tables, load each CSV",
    tags=["walmart"],
    doc_md=__doc__,
    template_searchpath=[INCLUDE_DIR],
)
def csv_to_bigquery():

    # Idempotent: logs and continues if the bucket already exists
    create_bucket = GCSCreateBucketOperator(
        task_id="create_bucket",
        bucket_name=BUCKET_NAME,
        location=BUCKET_LOCATION,
        storage_class="STANDARD",
        gcp_conn_id=GCP_CONN_ID,
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

    # DDL only: creates the dataset and empty tables (see walmart_schema.sql)
    create_tables = BigQueryInsertJobOperator(
        task_id="create_tables",
        configuration={
            "query": {
                "query": "{% include 'walmart_dataset/ddl/walmart_schema.sql' %}",
                "useLegacySql": False,
            }
        },
        location=BQ_LOCATION,
        gcp_conn_id=GCP_CONN_ID,
    )

    # One load task per table: separate logs and retries.
    # autodetect=False + explicit schema_fields keeps the column types from the DDL.
    # WRITE_TRUNCATE replaces the table contents, so reruns don't duplicate rows.
    load_tasks = [
        GCSToBigQueryOperator(
            task_id=f"load_{table}",
            bucket=BUCKET_NAME,
            source_objects=[f"raw/{table}.csv"],
            destination_project_dataset_table=f"{BQ_DATASET}.{table}",
            source_format="CSV",
            skip_leading_rows=1,
            autodetect=False,
            schema_fields=SCHEMAS[table],
            write_disposition="WRITE_TRUNCATE",
            location=BQ_LOCATION,
            gcp_conn_id=GCP_CONN_ID,
        )
        for table in TABLES
    ]

    create_bucket >> upload_csvs_to_gcs >> create_tables >> load_tasks


csv_to_bigquery()