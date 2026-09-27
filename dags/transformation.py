from datetime import datetime
from pathlib import Path
from airflow.operators.python import get_current_context
from airflow import DAG
from airflow.decorators import task
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
)


# =========================================================
# Configuration
# =========================================================

PROJECT_ID = "<GCP_PROJECT_ID>"

LOCATION = "US"

SOURCE_DATASET = "staging"
TARGET_DATASET = "curated"

SQL_DIR = Path(__file__).parent / "sql" / "transformations"


# =========================================================
# Transformation mapping
#
# task_id
# SQL file
# source staging table
# target curated table
# =========================================================

TRANSFORMATIONS = [
    (
        "transform_orders",
        "orders.sql",
        "olist_orders_dataset",
        "orders",
    ),
    (
        "transform_order_items",
        "order_items.sql",
        "olist_order_items_dataset",
        "order_items",
    ),
    (
        "transform_payments",
        "payments.sql",
        "olist_order_payments_dataset",
        "payments",
    ),
    (
        "transform_products",
        "products.sql",
        "olist_products_dataset",
        "products",
    ),
    (
        "transform_customers",
        "customers.sql",
        "olist_customers_dataset",
        "customers",
    ),
    (
        "transform_sellers",
        "sellers.sql",
        "olist_sellers_dataset",
        "sellers",
    ),
    (
        "transform_reviews",
        "reviews.sql",
        "olist_order_reviews_dataset",
        "reviews",
    ),
    (
        "transform_geolocation",
        "geolocation.sql",
        "olist_geolocation_dataset",
        "geolocation",
    ),
    (
        "transform_product_category_translation",
        "product_category_translation.sql",
        "product_category_name_translation",
        "product_category_translation",
    ),
]


# =========================================================
# DAG
# =========================================================

with DAG(
    dag_id="transformation",
    start_date=datetime(2026, 9, 26),
    schedule=None,
    catchup=False,
    tags=["transformation", "bigquery", "curated"],
) as dag:

    transformation_tasks = []
    audit_tasks = []

    # =====================================================
    # Create transformation + audit task pairs
    # =====================================================

    for (
        task_id,
        sql_file,
        source_table,
        target_table,
    ) in TRANSFORMATIONS:

        # -------------------------------------------------
        # Load SQL file
        # -------------------------------------------------

        sql_path = SQL_DIR / sql_file

        if not sql_path.exists():
            raise FileNotFoundError(
                f"SQL file not found: {sql_path}"
            )

        sql_query = sql_path.read_text(
            encoding="utf-8"
        )

        # -------------------------------------------------
        # Transformation task
        # -------------------------------------------------

        transformation_task = BigQueryInsertJobOperator(
            task_id=task_id,
            configuration={
                "query": {
                    "query": sql_query,
                    "useLegacySql": False,
                }
            },
            location=LOCATION,
            gcp_conn_id="google_cloud_default",
        )

        # -------------------------------------------------
        # Audit task
        # -------------------------------------------------

        @task(
            task_id=f"audit_{target_table}",
            max_active_tis_per_dag=1,
        )
        def audit_transformation(
            source_table,
            target_table,
        ):
            context = get_current_context()
            dag_run = context["dag_run"]

            pipeline_run_id = dag_run.conf.get(
                "pipeline_run_id",
                dag_run.run_id,
            )

            hook = BigQueryHook(
                gcp_conn_id="google_cloud_default",
                use_legacy_sql=False,
            )

            # =============================================
            # Source row count
            # =============================================

            source_query = f"""
                SELECT COUNT(*)
                FROM `{PROJECT_ID}.{SOURCE_DATASET}.{source_table}`
            """

            source_result = hook.get_first(
                source_query
            )

            source_row_count = source_result[0]

            # =============================================
            # Target row count
            # =============================================

            target_query = f"""
                SELECT COUNT(*)
                FROM `{PROJECT_ID}.{TARGET_DATASET}.{target_table}`
            """

            target_result = hook.get_first(
                target_query
            )

            target_row_count = target_result[0]

            # =============================================
            # Logging
            # =============================================

            print(
                f"Transformation audit: "
                f"{SOURCE_DATASET}.{source_table} "
                f"→ "
                f"{TARGET_DATASET}.{target_table}"
            )

            print(
                f"Source row count: "
                f"{source_row_count}"
            )

            print(
                f"Target row count: "
                f"{target_row_count}"
            )

            # =============================================
            # Audit validation
            # =============================================

            if target_row_count == 0:
                raise ValueError(
                    f"Transformation audit failed for "
                    f"{target_table}: "
                    f"target table is empty."
                )

            status = "PASS"

            # =============================================
            # Write audit result
            # =============================================

            audit_query = f"""
                INSERT INTO
                    `{PROJECT_ID}.audit.transformation_results`
                (
                    transformation_timestamp,
                    dag_id,
                    run_id,
                    source_dataset,
                    source_table,
                    target_dataset,
                    target_table,
                    source_row_count,
                    target_row_count,
                    status
                )
                VALUES
                (
                    CURRENT_TIMESTAMP(),
                    'transformation',
                    '{pipeline_run_id}',
                    '{SOURCE_DATASET}',
                    '{source_table}',
                    '{TARGET_DATASET}',
                    '{target_table}',
                    {source_row_count},
                    {target_row_count},
                    '{status}'
                )
            """

            print(
                f"Writing transformation audit for "
                f"{source_table} → {target_table}"
            )

            hook.run(audit_query)

            print(
                f"Transformation audit stored for "
                f"{target_table}"
            )

            # =============================================
            # Return result
            # =============================================

            return {
                "source_table": source_table,
                "target_table": target_table,
                "source_row_count": source_row_count,
                "target_row_count": target_row_count,
                "status": status,
            }

        audit_task = audit_transformation(
            source_table,
            target_table,
        )

        transformation_tasks.append(
            transformation_task
        )

        audit_tasks.append(
            audit_task
        )

    # =====================================================
    # Sequential dependency chain
    #
    # transform
    #     ↓
    # audit
    #     ↓
    # next transform
    #     ↓
    # next audit
    # =====================================================

    for i in range(
        len(transformation_tasks)
    ):

        # Transformation must finish
        # before its audit runs.

        transformation_tasks[i] >> audit_tasks[i]

        # Audit must finish
        # before the next transformation starts.

        if i < len(transformation_tasks) - 1:

            audit_tasks[i] >> transformation_tasks[
                i + 1
            ]