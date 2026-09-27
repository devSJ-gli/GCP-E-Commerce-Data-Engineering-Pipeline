from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import get_current_context
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook


# =========================================================
# Configuration
# =========================================================

PROJECT_ID = "<GCP_PROJECT_ID>"
DATASET_ID = "curated"
LOCATION = "US"


# =========================================================
# DAG
# =========================================================

with DAG(
    dag_id="curated_validation",
    start_date=datetime(2026, 9, 26),
    schedule=None,
    catchup=False,
    tags=["validation", "bigquery", "curated"],
) as dag:

    # =====================================================
    # Table validation configuration
    # =====================================================

    table_configs = [

        {
            "table": "orders",
            "key_columns": ["order_id"],
            "null_columns": ["order_id"],
            "duplicate_check": True,
            "range_checks": [],
        },

        {
            "table": "order_items",
            "key_columns": ["order_id", "order_item_id"],
            "null_columns": [
                "order_id",
                "order_item_id",
                "product_id",
                "seller_id",
                "price",
                "freight_value",
            ],
            "duplicate_check": True,
            "range_checks": [
                ("price", ">= 0"),
                ("freight_value", ">= 0"),
                ("item_total_value", ">= 0"),
            ],
        },

        {
            "table": "payments",
            "key_columns": ["order_id", "payment_sequential"],
            "null_columns": [
                "order_id",
                "payment_sequential",
                "payment_type",
                "payment_value",
            ],
            "duplicate_check": True,
            "range_checks": [
                ("payment_value", ">= 0"),
                ("payment_installments", ">= 0"),
            ],
        },

        {
            "table": "products",
            "key_columns": ["product_id"],
            "null_columns": ["product_id"],
            "duplicate_check": True,
            "range_checks": [
                ("product_weight_g", ">= 0"),
                ("product_length_cm", ">= 0"),
                ("product_height_cm", ">= 0"),
                ("product_width_cm", ">= 0"),
                ("product_volume_cm3", ">= 0"),
            ],
        },

        {
            "table": "customers",
            "key_columns": ["customer_id"],
            "null_columns": [
                "customer_id",
                "customer_unique_id",
                "customer_city",
                "customer_state",
            ],
            "duplicate_check": True,
            "range_checks": [],
        },

        {
            "table": "sellers",
            "key_columns": ["seller_id"],
            "null_columns": [
                "seller_id",
                "seller_city",
                "seller_state",
            ],
            "duplicate_check": True,
            "range_checks": [],
        },

        {
            "table": "reviews",
            "key_columns": ["review_id", "order_id"],
            "null_columns": [
                "review_id",
                "order_id",
                "review_score",
            ],
            "duplicate_check": True,
            "range_checks": [
                ("review_score", "BETWEEN 1 AND 5"),
            ],
        },

        {
            "table": "geolocation",
            "key_columns": [],
            "null_columns": [
                "geolocation_zip_code_prefix",
                "geolocation_lat",
                "geolocation_lng",
                "geolocation_city",
                "geolocation_state",
            ],
            "duplicate_check": False,
            "range_checks": [
                ("geolocation_lat", "BETWEEN -90 AND 90"),
                ("geolocation_lng", "BETWEEN -180 AND 180"),
            ],
        },

        {
            "table": "product_category_translation",
            "key_columns": ["product_category_name"],
            "null_columns": [
                "product_category_name",
                "product_category_name_english",
            ],
            "duplicate_check": True,
            "range_checks": [],
        },
    ]


    # =====================================================
    # Validation task
    # =====================================================

    @task(max_active_tis_per_dag=1)
    def validate_table(config):

        # -------------------------------------------------
        # Get Airflow runtime context
        # -------------------------------------------------

        context = get_current_context()

        dag_run = context["dag_run"]

        # -------------------------------------------------
        # Get pipeline run ID
        #
        # If Master DAG triggers this DAG, it should pass:
        #
        # {
        #     "pipeline_run_id": "..."
        # }
        #
        # When this DAG is run manually, we fall back to
        # its own Airflow run_id.
        # -------------------------------------------------

        pipeline_run_id = dag_run.conf.get(
            "pipeline_run_id",
            dag_run.run_id,
        )

        table = config["table"]
        key_columns = config["key_columns"]
        null_columns = config["null_columns"]
        duplicate_check = config["duplicate_check"]
        range_checks = config["range_checks"]

        # -------------------------------------------------
        # BigQuery hook
        # -------------------------------------------------

        hook = BigQueryHook(
            gcp_conn_id="google_cloud_default",
            use_legacy_sql=False,
        )

        # =================================================
        # NULL checks
        # =================================================

        null_expression = " + ".join(
            [
                f"COUNTIF(`{column}` IS NULL)"
                for column in null_columns
            ]
        )

        if not null_expression:
            null_expression = "0"

        # =================================================
        # Duplicate checks
        # =================================================

        if duplicate_check and key_columns:

            key_expression = ", ".join(
                f"`{column}`"
                for column in key_columns
            )

            duplicate_expression = f"""
                (
                    SELECT COUNT(*)
                    FROM (
                        SELECT
                            {key_expression},
                            COUNT(*) AS duplicate_count
                        FROM `{PROJECT_ID}.{DATASET_ID}.{table}`
                        GROUP BY {key_expression}
                        HAVING COUNT(*) > 1
                    )
                )
            """

        else:
            duplicate_expression = "0"

        # =================================================
        # Range checks
        # =================================================

        range_expressions = []

        for column, condition in range_checks:

            range_expressions.append(
                f"COUNTIF(`{column}` IS NOT NULL "
                f"AND NOT (`{column}` {condition}))"
            )

        if range_expressions:
            range_expression = " + ".join(
                range_expressions
            )
        else:
            range_expression = "0"

        # =================================================
        # Main validation query
        # =================================================

        query = f"""
            SELECT
                COUNT(*) AS row_count,

                {null_expression}
                    AS null_count,

                {duplicate_expression}
                    AS duplicate_count,

                {range_expression}
                    AS range_violation_count

            FROM `{PROJECT_ID}.{DATASET_ID}.{table}`
        """

        print(f"Validating table: {table}")
        print(query)

        # -------------------------------------------------
        # Execute validation
        # -------------------------------------------------

        results = hook.get_first(query)

        row_count = results[0]
        null_count = results[1]
        duplicate_count = results[2]
        range_violation_count = results[3]

        # -------------------------------------------------
        # Logging
        # -------------------------------------------------

        print(f"Table: {table}")
        print(f"Row count: {row_count}")
        print(f"Null count: {null_count}")
        print(f"Duplicate count: {duplicate_count}")
        print(
            f"Range violations: "
            f"{range_violation_count}"
        )

        print(
            f"Pipeline run ID: "
            f"{pipeline_run_id}"
        )

        # =================================================
        # Validation decisions
        # =================================================

        if row_count == 0:
            raise ValueError(
                f"Validation failed for {table}: "
                "table is empty."
            )

        if null_count > 0:
            raise ValueError(
                f"Validation failed for {table}: "
                f"{null_count} NULL values found."
            )

        if duplicate_count > 0:
            raise ValueError(
                f"Validation failed for {table}: "
                f"{duplicate_count} duplicate key groups found."
            )

        if range_violation_count > 0:
            raise ValueError(
                f"Validation failed for {table}: "
                f"{range_violation_count} range violations found."
            )

        print(
            f"Validation PASSED for {table}"
        )

        # =================================================
        # Persist successful validation result
        # =================================================

        audit_query = f"""
            INSERT INTO
                `{PROJECT_ID}.audit.validation_results`
            (
                validation_timestamp,
                dag_id,
                run_id,
                validation_layer,
                table_name,
                status,
                row_count,
                null_count,
                duplicate_count,
                range_violation_count
            )
            VALUES
            (
                CURRENT_TIMESTAMP(),
                'curated_validation',
                '{pipeline_run_id}',
                'curated',
                '{table}',
                'PASS',
                {row_count},
                {null_count},
                {duplicate_count},
                {range_violation_count}
            )
        """

        print(
            f"Writing audit result for {table}"
        )

        hook.run(audit_query)

        print(
            f"Audit result stored for {table}"
        )

        # =================================================
        # Return validation result
        # =================================================

        return {
            "table": table,
            "status": "PASS",
            "row_count": row_count,
            "null_count": null_count,
            "duplicate_count": duplicate_count,
            "range_violation_count":
                range_violation_count,
            "run_id": pipeline_run_id,
        }


    # =====================================================
    # Dynamic validation for every configured table
    # =====================================================

    validate_table.expand(
        config=table_configs
    )