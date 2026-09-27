from datetime import datetime
from pathlib import Path

import yaml

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import get_current_context
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook


# =========================================================
# Configuration
# =========================================================

PROJECT_ID = "<GCP_PROJECT_ID>"

DATASET_ID = "staging"

LOCATION = "US"


# =========================================================
# Load validation configuration from YAML
# =========================================================

CONFIG_PATH = (
    Path(__file__).resolve().parent
    / "config"
    / "table_config.yaml"
)

with open(CONFIG_PATH, "r") as file:
    yaml_config = yaml.safe_load(file)


# =========================================================
# Convert YAML configuration into validation config
# =========================================================

table_configs = []

for table_name, config in yaml_config["tables"].items():

    table_configs.append(
        {
            "table": table_name,
            "key_columns": config["key_columns"],
            "null_check": config["checks"]["null_check"],
            "duplicate_check": config["checks"]["duplicate_check"],
        }
    )


# =========================================================
# DAG
# =========================================================

with DAG(
    dag_id="validation",
    start_date=datetime(2026, 9, 26),
    schedule=None,
    catchup=False,
    tags=["validation", "bigquery", "staging"],
) as dag:

    # =====================================================
    # Validate one staging table
    # =====================================================

    @task(
        max_active_tis_per_dag=1
    )
    def validate_table(config):

        # -------------------------------------------------
        # Airflow context
        # -------------------------------------------------

        context = get_current_context()

        dag_run = context["dag_run"]

        # -------------------------------------------------
        # Pipeline run ID
        #
        # When called from the Master DAG:
        # use the pipeline_run_id passed through dag_run.conf.
        #
        # When run manually:
        # fall back to this DAG's own run_id.
        # -------------------------------------------------

        pipeline_run_id = dag_run.conf.get(
            "pipeline_run_id",
            dag_run.run_id,
        )

        # -------------------------------------------------
        # Table configuration
        # -------------------------------------------------

        table = config["table"]

        key_columns = config["key_columns"]

        # -------------------------------------------------
        # NULL validation
        # -------------------------------------------------

        null_conditions = []

        for column in key_columns:

            null_conditions.append(
                f"COUNTIF(`{column}` IS NULL)"
            )

        null_expression = " + ".join(
            null_conditions
        )

        # -------------------------------------------------
        # Duplicate validation
        # -------------------------------------------------

        duplicate_expression = "0"

        if config["duplicate_check"]:

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

        # -------------------------------------------------
        # Validation query
        # -------------------------------------------------

        query = f"""
            SELECT
                COUNT(*) AS row_count,
                {null_expression} AS null_count,
                {duplicate_expression} AS duplicate_count
            FROM `{PROJECT_ID}.{DATASET_ID}.{table}`
        """

        # -------------------------------------------------
        # BigQuery connection
        # -------------------------------------------------

        hook = BigQueryHook(
            gcp_conn_id="google_cloud_default",
            use_legacy_sql=False,
        )

        # -------------------------------------------------
        # Execute validation query
        # -------------------------------------------------

        result = hook.get_first(query)

        row_count = result[0]

        null_count = result[1]

        duplicate_count = result[2]

        # -------------------------------------------------
        # Logging
        # -------------------------------------------------

        print(
            f"Pipeline run ID: {pipeline_run_id}"
        )

        print(
            f"Table: {table}"
        )

        print(
            f"Row count: {row_count}"
        )

        print(
            f"Null count: {null_count}"
        )

        print(
            f"Duplicate count: {duplicate_count}"
        )

        # -------------------------------------------------
        # Validation failures
        # -------------------------------------------------

        if row_count == 0:

            raise ValueError(
                f"Validation failed for {table}: "
                "table is empty."
            )

        if (
            config["null_check"]
            and null_count > 0
        ):

            raise ValueError(
                f"Validation failed for {table}: "
                f"{null_count} NULL key values found."
            )

        if (
            config["duplicate_check"]
            and duplicate_count > 0
        ):

            raise ValueError(
                f"Validation failed for {table}: "
                f"{duplicate_count} duplicate key groups found."
            )

        # -------------------------------------------------
        # Validation passed
        # -------------------------------------------------

        print(
            f"Validation PASSED for {table}"
        )

        # -------------------------------------------------
        # Persist validation result
        # -------------------------------------------------

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
                'validation',
                '{pipeline_run_id}',
                'staging',
                '{table}',
                'PASS',
                {row_count},
                {null_count},
                {duplicate_count},
                NULL
            )
        """

        print(
            f"Writing audit result for {table}"
        )

        hook.run(audit_query)

        print(
            f"Audit result stored for {table}"
        )

        # -------------------------------------------------
        # Return validation result
        # -------------------------------------------------

        return {
            "table": table,
            "status": "PASS",
            "row_count": row_count,
            "null_count": null_count,
            "duplicate_count": duplicate_count,
            "pipeline_run_id": pipeline_run_id,
        }

    # =====================================================
    # Dynamic validation for every configured table
    # =====================================================

    validate_table.expand(
        config=table_configs
    )