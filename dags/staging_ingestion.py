from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param
from airflow.providers.google.cloud.operators.gcs import (
    GCSListObjectsOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from google.cloud import bigquery


with DAG(
    dag_id="gcs_to_bq",
    start_date=datetime(2026, 9, 26),
    schedule=None,
    catchup=False,

    params={
        "bucket": Param(
            "",
            type="string",
            title="GCS Bucket",
            description="GCS bucket containing the source files.",
        ),
        "folder": Param(
            "",
            type="string",
            title="GCS Folder",
            description="GCS folder/prefix containing the source files.",
        ),
    },

    tags=["gcs", "bigquery", "ingestion"],
) as dag:

    # ---------------------------------------------------------
    # 1. List files from GCS
    # ---------------------------------------------------------
    list_files = GCSListObjectsOperator(
        task_id="list_gcs_files",
        bucket="{{ params.bucket }}",
        prefix="{{ params.folder }}",
        gcp_conn_id="google_cloud_default",
    )

    # ---------------------------------------------------------
    # 2. Build BigQuery load configuration for each file
    # ---------------------------------------------------------
    @task
    def build_load_configs(files):
        configs = []

        supported_formats = {
            ".csv": "CSV",
            ".json": "NEWLINE_DELIMITED_JSON",
            ".parquet": "PARQUET",
            ".avro": "AVRO",
            ".orc": "ORC",
        }

        client = bigquery.Client()
        project_id = client.project

        for file_path in files:

            # Skip folders
            if file_path.endswith("/"):
                continue

            filename = file_path.split("/")[-1]

            # Skip files without extensions
            if "." not in filename:
                continue

            extension = "." + filename.rsplit(".", 1)[1].lower()

            # Skip unsupported formats
            if extension not in supported_formats:
                continue

            source_format = supported_formats[extension]

            # Create BigQuery table name from filename
            table_name = filename.rsplit(".", 1)[0]

            table_name = (
                table_name
                .lower()
                .replace("-", "_")
                .replace(" ", "_")
            )

            configs.append(
                {
                    "source_objects": [file_path],

                    "destination_project_dataset_table": (
                        f"{project_id}.staging.{table_name}"
                    ),

                    "source_format": source_format,

                    "skip_leading_rows": (
                        1 if source_format == "CSV" else 0
                    ),

                    "autodetect": (
                        True
                        if source_format
                        in ["CSV", "NEWLINE_DELIMITED_JSON"]
                        else False
                    ),

                    # Required for CSV files containing
                    # newlines inside quoted fields.
                    "allow_quoted_newlines": (
                        True if source_format == "CSV" else False
                    ),

                    # Existing table:
                    # truncate and reload.
                    "write_disposition": "WRITE_TRUNCATE",

                    # Missing table:
                    # create automatically.
                    "create_disposition": "CREATE_IF_NEEDED",
                }
            )

        if not configs:
            raise ValueError(
                "No supported files were found in the supplied GCS folder."
            )

        return configs

    load_configs = build_load_configs(
        list_files.output
    )

    # ---------------------------------------------------------
    # 3. Load files into BigQuery
    #
    # Only ONE mapped task runs at a time.
    # ---------------------------------------------------------
    load_files = GCSToBigQueryOperator.partial(
        task_id="load_file_to_bigquery",

        bucket="{{ params.bucket }}",

        gcp_conn_id="google_cloud_default",

        max_active_tis_per_dag=1,

    ).expand_kwargs(load_configs)