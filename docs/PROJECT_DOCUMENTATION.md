# GCP E-Commerce Data Engineering Pipeline — Detailed Engineering Documentation

## 1. Executive Summary

This project implements a layered batch e-commerce data pipeline on GCP
using Cloud Storage, BigQuery, and Cloud Composer / Apache Airflow.

The pipeline separates:

-   raw storage
-   staging
-   validation
-   transformation
-   curated data
-   curated validation
-   presentation

Airflow is responsible for orchestration. BigQuery is responsible for
warehouse storage, SQL transformations, validation queries, audit
persistence, and analytical presentation models.

The final verified master execution successfully completed the
downstream pipeline:

``` 
validation
    ->
transformation
    ->
curated_validation
    ->
presentation
```

The ingestion DAG was independently verified before the master run.

------------------------------------------------------------------------

# 2. Environment

## GCP

GCP project ID:

``` 
<GCP_PROJECT_ID>
```

Region / location used by BigQuery:

``` 
US
```

GCS bucket:

``` 
project-olist-data
```

Raw prefix:

``` 
raw/ecom/
```

BigQuery datasets:

``` 
staging
curated
presentation
audit
```

## Cloud Composer

Environment:

``` 
project-airflow
```

Generation:

``` 
Composer Gen 3
```

Airflow:

``` 
2.11.1-build.19
```

Region:

``` 
asia-south1
```

Composer service account:

``` 
<COMPOSER_SERVICE_ACCOUNT>
```

------------------------------------------------------------------------

# 3. Layer-by-Layer Implementation

## Layer 0 --- Raw Cloud Storage

Source files were placed under:

``` 
gs://project-olist-data/raw/ecom/
```

The raw layer is intentionally kept separate from BigQuery
transformations.

The ingestion process discovers files instead of hardcoding the nine
Olist filenames.

------------------------------------------------------------------------

# 4. Ingestion DAG

**Evidence screenshot:** `screenshots/Airflow/01_gcs_to_bq_success.png`
Show the successful `gcs_to_bq` DAG run with the mapped ingestion tasks.

**Evidence screenshot:** `screenshots/GCS/01_raw_ecom_files.png`
Show the `raw/ecom/` prefix and the nine source files.

File:

``` 
staging_ingestion.py
```

DAG:

``` 
gcs_to_bq
```

### Main operators

``` python
GCSListObjectsOperator
GCSToBigQueryOperator
```

### Dynamic discovery

The DAG first lists objects:

``` python
list_files = GCSListObjectsOperator(
    task_id="list_gcs_files",
    bucket="{{ params.bucket }}",
    prefix="{{ params.folder }}",
    gcp_conn_id="google_cloud_default",
)
```

The returned file list is passed into a TaskFlow function that builds
BigQuery load configurations.

### Supported formats

``` python
supported_formats = {
    ".csv": "CSV",
    ".json": "NEWLINE_DELIMITED_JSON",
    ".parquet": "PARQUET",
    ".avro": "AVRO",
    ".orc": "ORC",
}
```

### Load behavior

``` python
"write_disposition": "WRITE_TRUNCATE"
"create_disposition": "CREATE_IF_NEEDED"
```

This makes the current project a full-refresh batch pipeline.

### Important CSV setting

``` python
"allow_quoted_newlines": True
```

This was added after the reviews dataset failed ingestion because quoted
text fields contained newline characters.

### Why only one mapped task at a time?

During development, increasing concurrency caused transient Composer
metadata/database instability.

The project therefore used:

``` python
max_active_tis_per_dag=1
```

for mapped processing where controlled execution was preferable.

------------------------------------------------------------------------

# 5. Staging Layer

**Evidence screenshot:** `screenshots/BigQuery/01_staging_tables.png`
Show the nine tables in the `staging` dataset.

Nine source tables were successfully loaded.

``` 
olist_orders_dataset
olist_order_items_dataset
olist_order_payments_dataset
olist_products_dataset
olist_customers_dataset
olist_sellers_dataset
olist_order_reviews_dataset
olist_geolocation_dataset
product_category_name_translation
```

The ingestion layer was tested independently before being removed from
the master DAG's downstream orchestration path.

------------------------------------------------------------------------

# 6. Staging Validation

**Evidence screenshot:** `screenshots/Airflow/02_validation_success.png`
Show the mapped validation tasks completed successfully.

**Verification:** See `docs/VERIFICATION_QUERIES.sql` (staging validation query) and the corresponding evidence screenshot above.

File:

``` 
validation.py
```

DAG:

``` 
validation
```

The validation configuration is stored in:

``` 
dags/config/table_config.yaml
```

`config/` and `sql/` are nested under `dags/` (rather than living as
top-level folders) because Cloud Composer only syncs the `dags/` folder
from the environment's bucket to the workers. Every file a DAG opens with
a relative path at parse/run time — `table_config.yaml`, the
transformation and presentation `.sql` files — has to live inside that
same synced tree, so `validation.py`, `transformation.py`, and
`presentation.py` all resolve their paths as `Path(__file__).parent / ...`.

This avoids embedding all table rules directly inside the validation
task.

### Example configuration

``` yaml
olist_order_items_dataset:
  key_columns:
    - order_id
    - order_item_id
  checks:
    null_check: true
    duplicate_check: true
```

### Dynamic validation

``` python
validate_table.expand(
    config=table_configs
)
```

This creates one mapped validation task per configured table.

### Main validation logic

The task calculates:

``` 
row_count
null_count
duplicate_count
```

Then applies:

``` python
if row_count == 0:
    raise ValueError(...)

if null_check and null_count > 0:
    raise ValueError(...)

if duplicate_check and duplicate_count > 0:
    raise ValueError(...)
```

Successful results are inserted into:

``` 
audit.validation_results
```

------------------------------------------------------------------------

# 7. Validation Key Design

One of the most important debugging lessons came from the reviews table.

Initial assumption:

``` 
review_id
```

was unique.

The validation identified duplicate groups.

Instead of immediately removing the duplicate check, the data was
investigated.

The actual logical uniqueness for this project was:

``` 
(review_id, order_id)
```

The configuration was changed to:

``` yaml
olist_order_reviews_dataset:
  key_columns:
    - review_id
    - order_id
  checks:
    null_check: true
    duplicate_check: true
```

The final validation passed.

This is an example of why data-quality rules must be derived from the
data model rather than from assumptions about column names.

------------------------------------------------------------------------

# 8. Curated Transformation Layer

**Evidence screenshot:** `screenshots/Airflow/03_transformation_success.png`
Show the transformation DAG and successful transformation/audit tasks.

**Evidence screenshot:** `screenshots/BigQuery/02_curated_tables.png`
Show the nine tables in the `curated` dataset.

File:

``` 
transformation.py
```

DAG:

``` 
transformation
```

Main operator:

``` python
BigQueryInsertJobOperator
```

SQL files are stored under:

``` 
dags/sql/transformations/
```

## Explicit source-to-target mapping

``` python
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
```

This explicit mapping solved an audit failure caused by assuming staging
and curated table names were identical.

------------------------------------------------------------------------

# 9. Transformation Audit

Every transformation has a corresponding audit task.

The audit reads:

``` sql
SELECT COUNT(*)
FROM `project.dataset.source_table`
```

and:

``` sql
SELECT COUNT(*)
FROM `project.dataset.target_table`
```

Then checks that the target is not empty.

The audit stores:

``` 
source_row_count
target_row_count
status
run_id
```

in:

``` 
audit.transformation_results
```

Final result:

``` 
9 transformations
9 PASS
```

Source and target row counts matched for all nine transformations.

------------------------------------------------------------------------

# 10. Curated Validation

**Evidence screenshot:** `screenshots/Airflow/04_curated_validation_success.png`
Show the nine curated validation tasks completed successfully.

**Verification:** See `docs/VERIFICATION_QUERIES.sql` (curated validation query) and the corresponding evidence screenshot above.

File:

``` 
curated_validation.py
```

DAG:

``` 
curated_validation
```

This is more comprehensive than staging validation.

Checks include:

### NULL checks

Examples:

``` 
order_id
product_id
seller_id
price
payment_value
review_score
```

### Duplicate checks

Examples:

``` 
orders:
order_id

order_items:
(order_id, order_item_id)

payments:
(order_id, payment_sequential)

reviews:
(review_id, order_id)
```

### Range checks

Examples:

``` 
price >= 0
freight_value >= 0
payment_value >= 0
review_score BETWEEN 1 AND 5
latitude BETWEEN -90 AND 90
longitude BETWEEN -180 AND 180
```

The curated validation DAG uses:

``` python
get_current_context()
```

from:

``` python
from airflow.operators.python import get_current_context
```

An earlier import from the wrong Airflow module caused a broken DAG and
was corrected.

------------------------------------------------------------------------

# 11. Presentation Layer

**Evidence screenshot:** `screenshots/Airflow/05_presentation_success.png`
Show the four presentation tasks completed successfully.

**Evidence screenshot:** `screenshots/BigQuery/03_presentation_tables.png`
Show the four presentation tables.

File:

``` 
presentation.py
```

DAG:

``` 
presentation
```

The DAG executes four SQL models:

``` 
sales_summary.sql
customer_summary.sql
product_summary.sql
delivery_summary.sql
```

Operator:

``` python
BigQueryInsertJobOperator
```

The presentation tasks are deliberately sequential:

``` python
for upstream, downstream in zip(tasks, tasks[1:]):
    upstream >> downstream
```

This is not because the four models have strict data dependencies; it
was chosen for controlled execution and easier debugging in this small
portfolio pipeline.

------------------------------------------------------------------------

# 12. Sales Model

The sales model groups by:

``` 
purchase_year
purchase_month
order_status
```

and calculates:

``` 
order_count
item_count
product_revenue
freight_revenue
total_revenue
average_order_value
```

A key SQL design decision was avoiding a direct:

``` 
orders -> order_items -> payments
```

aggregation.

Because both items and payments can be one-to-many with orders, joining
all three without pre-aggregation can multiply rows and inflate revenue.

The implemented sales model therefore uses the order and item
relationship for the selected metrics.

------------------------------------------------------------------------

# 13. Presentation Verification

Final row counts:

``` 
sales_summary       111
customer_summary    99441
product_summary     32951
delivery_summary    99441
```

All four tables were queried directly and returned populated analytical data.

**Evidence screenshot:** `screenshots/Evidence/10_presentation_content.png`


------------------------------------------------------------------------

# 14. Product Category Completeness

The product presentation model contained some NULL category values.

This was quantified rather than guessed.

Final result:

``` 
total_products = 32951
products_without_category = 610
NULL category rate = 1.85%
```

The missing category values were not automatically replaced because the
project treats source-data completeness separately from transformation
correctness.


# 15. Measured Execution Evidence

The final verification measurements were captured from the actual GCP environment used for the pipeline
GCP environment. Values below are measured results, not estimates.

### Raw GCS

``` 
Source CSV files: 9
Raw size: 126,186,995 bytes
Raw size: 120.34 MiB
```

### BigQuery storage

Storage was measured with `bq show` because the current identity did not have
permission to query `region-us.INFORMATION_SCHEMA.TABLE_STORAGE`.

``` 
staging       logical: 99,794,941 bytes    physical: 179,642,638 bytes
curated       logical: 107,511,330 bytes   physical: 252,943,217 bytes
presentation  logical: 29,585,420 bytes    physical: 27,120,864 bytes
audit         logical: 11,348 bytes        physical: 16,100 bytes
```

### Validation

``` 
Staging validation:  9/9 PASS
Curated validation:  9/9 PASS
```

The configured validation checks produced zero NULL, duplicate, and range
violations in the final verified run.

### Transformation

``` 
9/9 transformations PASS
```

All source-to-target row counts matched.

### Run correlation

``` 
manual__2026-09-26T14:50:21.327486+00:00
```

For this run:

``` 
staging validation:      9 records
curated validation:      9 records
transformation audit:    9 records
```

### Data-quality measurements

``` 
Review duplicate composite-key groups: 0
Products without category: 610 / 32,951
NULL category rate: 1.85%
```

### Audit table totals

The audit tables contain historical records from multiple runs:

``` 
audit.validation_results:      54 rows
audit.transformation_results:  43 rows
```

### BigQuery job evidence

The BigQuery job verification query returned completed verification jobs.
The captured evidence included a largest observed job of:

``` 
6,742,232 bytes processed
```

The displayed job durations were rounded to zero seconds, so an end-to-end
pipeline runtime is not reported from this query.

### Evidence files

Detailed verification queries:

``` 
docs/VERIFICATION_QUERIES.sql
```

Measured results:

``` 
docs/results/MEASUREMENTS.md
```


------------------------------------------------------------------------

# 16. Master DAG

File:

``` 
master_dag.py
```

Operator:

``` python
TriggerDagRunOperator
```

The final downstream orchestration is:

``` 
validation
    |
    v
transformation
    |
    v
curated_validation
    |
    v
presentation
```

Each trigger uses:

``` python
wait_for_completion=True
```

and:

``` python
poke_interval=10
```

The master passes:

``` python
conf={
    "pipeline_run_id": PIPELINE_RUN_ID,
}
```

to each child DAG.

------------------------------------------------------------------------

# 17. Why Ingestion Was Excluded From the Master

The ingestion DAG has an explicit trigger contract:

``` 
bucket
folder
```

as Airflow parameters.

The first master execution attempted to trigger it without those values.

The child DAG consequently logged:

``` 
Bucket:
Prefix(es):
```

and failed because the bucket name was empty.

The correct engineering response was to identify the interface mismatch
rather than modify a working ingestion DAG without need.

The final design keeps:

``` 
gcs_to_bq
```

as a separately executable ingestion DAG.

The master orchestrates the downstream warehouse pipeline after staging
data exists.

------------------------------------------------------------------------

# 18. Run Correlation

The master creates:

``` python
PIPELINE_RUN_ID = "{{ dag_run.run_id }}"
```

Child DAGs retrieve:

``` python
pipeline_run_id = dag_run.conf.get(
    "pipeline_run_id",
    dag_run.run_id,
)
```

This was initially complicated by an incorrect nested `.get()`
expression. The simpler expression above was restored.

Final verified run:

``` 
manual__2026-09-26T14:50:21.327486+00:00
```

Audit verification:

``` 
staging validation       9 records
curated validation       9 records
transformation audit     9 records
```

Each group had:

``` 
distinct_run_ids = 1
```

This confirms cross-DAG run correlation.

------------------------------------------------------------------------

# 19. Error and Recovery Matrix

  -----------------------------------------------------------------------------------------------------------------
  Area             Failure                    Root cause              Fix                            Lesson
  ---------------- -------------------------- ----------------------- ------------------------------ --------------
  BigQuery         Dataset missing            Staging dataset not     Create dataset                 Provision
                                              provisioned                                            dependencies

  IAM              `bigquery.jobs.create`     Missing job permission  BigQuery Job User              Job vs data
                                                                                                     permissions
                                                                                                     differ

  IAM              `bigquery.tables.create`   Missing dataset write   BigQuery Data Editor           Dataset
                                              permission                                             permissions
                                                                                                     matter

  GCS/CSV          Reviews load failed        Embedded newlines       `allow_quoted_newlines=True`   Real CSV is
                                                                                                     messy

  Airflow          Metadata instability       Excessive mapped        Limit active tasks             Tune
                                              concurrency                                            concurrency

  Airflow          Mapped tasks appeared      Scheduler/resource      Revert concurrency + fresh run Isolate infra
                   stuck                      state                                                  vs code issues

  Validation       Review duplicates          Wrong uniqueness        Composite key                  Model data
                                              assumption                                             semantics

  Transformation   Audit mapping failure      Name mismatch           Explicit mapping               Avoid implicit
                                                                                                     conventions

  Airflow          Broken DAG                 Wrong                   Correct Airflow import         Match
                                              `get_current_context`                                  installed
                                              import                                                 Airflow API

  Run tracking     Bad run ID expression      Incorrect nested        Simple fallback                Prefer simple
                                              `.get()`                                               defensive code

  Master           Ingestion failure          Missing bucket/folder   Keep ingestion separate        Define DAG
                                              params                                                 interfaces
  -----------------------------------------------------------------------------------------------------------------

------------------------------------------------------------------------

# 20. What Was Learned

## Data engineering

1.  Data layers should have distinct responsibilities.
2.  Raw data should not be transformed in place.
3.  Validation should occur at meaningful boundaries.
4.  Audit data is different from application logs.
5.  Source-to-target mappings should be explicit.
6.  Row-count reconciliation is a useful basic transformation check.
7.  Business keys must be based on actual data semantics.
8.  One-to-many joins can silently corrupt aggregate metrics.
9.  Full refresh is simpler but has different operational
    characteristics from incremental processing.

## Airflow

1.  A DAG is an orchestration graph, not the warehouse itself.
2.  Operators perform work; dependencies determine order.
3.  Dynamic task mapping is useful for table/file-level processing.
4.  `TriggerDagRunOperator` can compose multiple DAGs into a
    higher-level workflow.
5.  Child DAGs need a clear input contract.
6.  Concurrency must be tuned for the environment.
7.  Airflow logs are essential for failure diagnosis.
8.  Run IDs provide useful cross-DAG correlation.

## GCP

1.  IAM permissions are granular.
2.  BigQuery job execution and dataset modification are separate
    permissions.
3.  Dataset location matters.
4.  Cloud Composer is managed infrastructure with its own resource
    constraints.
5.  GCS is well suited to a raw landing layer.
6.  BigQuery is sufficient for the SQL-based batch workload used in this project.

------------------------------------------------------------------------

------------------------------------------------------------------------

# 21. Final Verification Checklist

``` 
[x] GCS raw files available
[x] Staging dataset created
[x] Composer service account permissions configured
[x] 9 staging tables loaded
[x] Staging validation passed
[x] 9 curated transformations completed
[x] Transformation row counts reconciled
[x] Curated validation passed
[x] 4 presentation tables created
[x] Presentation tables contain data
[x] Audit tables populated
[x] Shared run ID verified
[x] Downstream master DAG execution verified
[x] Major failures documented
[x] Lessons learned documented
[x] Measured verification results documented
[x] Evidence query set documented
```

------------------------------------------------------------------------

# 22. Current Scope Boundary

The project intentionally ends at:

``` 
Presentation-ready BigQuery tables
```

Looker is not required for the data-engineering objective and was not
implemented.

The presentation layer is already designed so a BI tool could consume it
later.

Presentation-layer audit records and other productionization improvements
are outside the current project scope.
