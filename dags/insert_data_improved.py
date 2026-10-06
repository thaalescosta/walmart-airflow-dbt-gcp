"""One-off initial load: Walmart CSVs -> GCS (raw/) -> BigQuery.

Flow:
    check_local_files -> create_bucket  -> [per table: upload -> load -> check_not_empty]
                      -> create_dataset ----------------------^

Every task is idempotent, so the whole DAG (or any single task) can be re-run safely.
"""
from datetime import timedelta
from pathlib import Path

from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCheckOperator,
    BigQueryCreateEmptyDatasetOperator,
)
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.sdk import TaskGroup, dag, task
from pendulum import datetime

# ---------------------------------------------------------------------------
# Configuration
# Plain constants are fine at module level. What you must avoid at the top level
# of a DAG file is anything slow or stateful (API calls, DB queries, Variable.get,
# reading big files), because the scheduler re-parses this file every few seconds.
# ---------------------------------------------------------------------------
CSV_DIR = Path("/usr/local/airflow/include/walmart_dataset/data")
BUCKET_NAME = "walmart-dataset1"
BUCKET_LOCATION = "us-east1"  # free-tier region for the bucket
BQ_LOCATION = "US"            # US multi-region can load from a bucket in any US region
BQ_DATASET = "walmart_db"
GCP_CONN_ID = "gcp"


# ---------------------------------------------------------------------------
# Schemas: the single source of truth. The load job creates each table from these
# (create_disposition=CREATE_IF_NEEDED), so there is no separate DDL to keep in sync.
# ---------------------------------------------------------------------------
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
    dag_id="csv_to_bigquery_improved",
    schedule=None,                         # manual trigger only: it's a one-off load
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,                     # two concurrent runs would race on WRITE_TRUNCATE
    dagrun_timeout=timedelta(hours=1),     # never let a run hang forever
    default_args={
        "owner": "data-eng",
        "retries": 2,                      # absorbs transient GCP/network errors
        "retry_delay": timedelta(minutes=1),
        "execution_timeout": timedelta(minutes=30),  # per task
    },
    description="Initial load: upload CSVs to GCS and load them into BigQuery",
    tags=["walmart", "initial-load"],
    doc_md=__doc__,                        # renders this docstring in the Airflow UI
)
def csv_to_bigquery():

    # Fail fast: verify every CSV exists *before* creating any cloud resource.
    # A plain-Python step like this is what TaskFlow's @task is for; the Google
    # operators below are used as-is because they already do the job.
    @task
    def check_local_files() -> None:
        missing = [t for t in SCHEMAS if not (CSV_DIR / f"{t}.csv").is_file()]
        if missing:
            raise FileNotFoundError(f"Missing CSVs in {CSV_DIR}: {', '.join(missing)}")

    # Both creates are idempotent and independent, so they run in parallel.
    create_bucket = GCSCreateBucketOperator(
        task_id="create_bucket",
        bucket_name=BUCKET_NAME,
        location=BUCKET_LOCATION,
        storage_class="STANDARD",
        gcp_conn_id=GCP_CONN_ID,
    )

    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_dataset",
        dataset_id=BQ_DATASET,
        location=BQ_LOCATION,
        exists_ok=True,
        gcp_conn_id=GCP_CONN_ID,
    )

    preflight = check_local_files()
    preflight >> [create_bucket, create_dataset]

    # One TaskGroup per table: it keeps the graph readable, and each table
    # uploads, loads and retries independently of the others. A plain loop is the
    # right tool here because the table list is static; use dynamic task mapping
    # (.partial().expand()) when the list is only known at run time.
    for table, schema in SCHEMAS.items():
        with TaskGroup(group_id=table):
            upload = LocalFilesystemToGCSOperator(
                task_id="upload",
                src=str(CSV_DIR / f"{table}.csv"),
                dst=f"raw/{table}.csv",
                bucket=BUCKET_NAME,
                mime_type="text/csv",
                gcp_conn_id=GCP_CONN_ID,
            )

            # WRITE_TRUNCATE replaces the table contents, so reruns never duplicate rows.
            load = GCSToBigQueryOperator(
                task_id="load",
                bucket=BUCKET_NAME,
                source_objects=[f"raw/{table}.csv"],
                destination_project_dataset_table=f"{BQ_DATASET}.{table}",
                source_format="CSV",
                skip_leading_rows=1,
                allow_quoted_newlines=True,   # text fields may contain line breaks
                autodetect=False,
                schema_fields=schema,
                write_disposition="WRITE_TRUNCATE",
                create_disposition="CREATE_IF_NEEDED",
                location=BQ_LOCATION,
                gcp_conn_id=GCP_CONN_ID,
                deferrable=True,              # frees the worker slot while BigQuery works
            )

            # Data-quality gate: a load can "succeed" and still produce an empty table
            # (empty CSV, wrong file). COUNT(*) = 0 is falsy, so the check fails.
            check_not_empty = BigQueryCheckOperator(
                task_id="check_not_empty",
                sql=f"SELECT COUNT(*) FROM `{BQ_DATASET}.{table}`",
                use_legacy_sql=False,
                location=BQ_LOCATION,
                gcp_conn_id=GCP_CONN_ID,
            )

            create_bucket >> upload >> load >> check_not_empty
            create_dataset >> load


csv_to_bigquery()