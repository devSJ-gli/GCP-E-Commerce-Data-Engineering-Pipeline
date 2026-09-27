from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator


PROJECT_ID = "<GCP_PROJECT_ID>"
LOCATION = "US"

SQL_DIR = Path(__file__).parent / "sql" / "presentation"


PRESENTATIONS = [
    ("create_sales_summary", "sales_summary.sql"),
    ("create_customer_summary", "customer_summary.sql"),
    ("create_product_summary", "product_summary.sql"),
    ("create_delivery_summary", "delivery_summary.sql"),
]


with DAG(
    dag_id="presentation",
    start_date=datetime(2026, 9, 26),
    schedule=None,
    catchup=False,
    tags=["presentation", "bigquery"],
) as dag:

    tasks = []

    for task_id, sql_file in PRESENTATIONS:

        sql_path = SQL_DIR / sql_file

        if not sql_path.exists():
            raise FileNotFoundError(
                f"SQL file not found: {sql_path}"
            )

        task = BigQueryInsertJobOperator(
            task_id=task_id,
            configuration={
                "query": {
                    "query": sql_path.read_text(encoding="utf-8"),
                    "useLegacySql": False,
                }
            },
            location=LOCATION,
            gcp_conn_id="google_cloud_default",
        )

        tasks.append(task)

    # Run presentation models sequentially
    for upstream, downstream in zip(tasks, tasks[1:]):
        upstream >> downstream