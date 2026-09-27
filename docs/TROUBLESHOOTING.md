# GCP E-Commerce Data Engineering Pipeline — Troubleshooting Log

This document records the actual failures encountered during development of the GCP E-Commerce Data Engineering Pipeline
development, their root causes, the smallest corrective changes applied, and
the engineering lessons from each incident.

Only issues actually encountered during development of the GCP E-Commerce Data Engineering Pipeline are documented
here; screenshots are included only when they were actually captured.

## 1. Missing BigQuery staging dataset

### Symptom

The ingestion DAG could not load the staging tables.

### Root cause

The `staging` dataset had not yet been created.

### Resolution

Created:

``` text
<GCP_PROJECT_ID>.staging
```

in US.

### Lesson

Provision warehouse dependencies before task execution, or make
infrastructure provisioning an explicit part of deployment.

------------------------------------------------------------------------

## 2. BigQuery job permission

### Symptom

``` text
bigquery.jobs.create
```

permission error.

### Root cause

The Composer service account could authenticate to GCP but was not
authorized to create BigQuery jobs at the project level.

### Resolution

Granted:

``` text
roles/bigquery.jobUser
```

to the Composer service account.

### Lesson

Authentication and authorization are different. A service account being
valid does not imply it can submit every type of GCP job.

------------------------------------------------------------------------

## 3. BigQuery table creation permission

### Symptom

The DAG could submit BigQuery work but could not create staging tables.

### Root cause

Missing dataset-level table creation/write permissions.

### Resolution

Granted BigQuery Data Editor access on the staging dataset.

### Lesson

Separate:

``` text
create/run a BigQuery job
```

from:

``` text
create/modify a BigQuery table
```

------------------------------------------------------------------------

## 4. Reviews CSV newline problem

### Symptom

The reviews CSV failed during ingestion.

### Root cause

Quoted text fields contained embedded newline characters.

### Resolution

Added:

``` python
allow_quoted_newlines=True
```

for CSV loads.

### Lesson

Delimited files can contain delimiters and newline characters inside
quoted fields. Ingestion configuration must reflect actual source
characteristics.

------------------------------------------------------------------------

## 5. Composer concurrency instability

### Symptom

Increasing mapped task concurrency caused transient metadata/database
instability.

### Resolution

Reduced active mapped task concurrency to one:

``` python
max_active_tis_per_dag=1
```

### Lesson

Parallelism should be introduced deliberately. For this small project,
controlled sequential execution is acceptable and easier to debug.

------------------------------------------------------------------------

## 6. Mapped tasks appearing stuck

### Symptom

Mapped tasks did not progress normally after concurrency changes.

### Resolution

Reverted the concurrency change and ran a fresh execution.

### Lesson

Check scheduler/resource state before rewriting application logic.

------------------------------------------------------------------------

## 7. Reviews duplicate validation false positive

### Symptom

The review validation reported duplicate groups when `review_id` was
used alone.

### Investigation

The same review ID can occur across different orders.

### Resolution

Changed the logical key from:

``` text
review_id
```

to:

``` text
review_id + order_id
```

### Lesson

Data quality is domain modeling, not just SQL syntax.

------------------------------------------------------------------------

## 8. Transformation audit mapping failure

### Symptom

Transformation execution succeeded but audit logic could not correctly
identify source tables.

### Root cause

The audit assumed:

``` text
source table name == target table name
```

which was false.

### Resolution

Introduced explicit metadata:

``` python
source_table
target_table
sql_file
task_id
```

### Lesson

Use explicit metadata when systems have different naming conventions.

------------------------------------------------------------------------

## 9. Product category table naming mismatch

### Symptom

The category transformation/audit used an incorrect source table name.

### Correct source

``` text
product_category_name_translation
```

### Correct curated target

``` text
product_category_translation
```

### Resolution

Updated the transformation mapping.

------------------------------------------------------------------------

## 10. Broken Airflow import

### Symptom

Curated validation appeared as a broken DAG.

### Root cause

`get_current_context` was imported from the wrong module.

### Correct import

``` python
from airflow.operators.python import get_current_context
```

### Lesson

Check the API against the actual installed Airflow version.

------------------------------------------------------------------------

## 11. Pipeline run ID coding error

### Incorrect version

``` python
pipeline_run_id = dag_run.conf.get(
    dag_run.conf.get("pipeline_run_id")
    if dag_run.conf
    else None
) or dag_run.run_id
```

### Correct version

``` python
pipeline_run_id = dag_run.conf.get(
    "pipeline_run_id",
    dag_run.run_id,
)
```

### Lesson

Avoid unnecessary defensive complexity when the Airflow object already
provides a clean fallback API.

------------------------------------------------------------------------

## 12. Master DAG ingestion failure

### Symptom

The master waited for `gcs_to_bq`, which failed.

Relevant log behavior:

``` text
Bucket:
Prefix(es):
```

followed by an error caused by the empty bucket name.

### Root cause

The ingestion DAG expects:

``` text
params.bucket
params.folder
```

but the master supplied only:

``` text
dag_run.conf.pipeline_run_id
```

### Resolution

The ingestion DAG was kept separate.

The master now focuses on:

``` text
validation
    ->
transformation
    ->
curated_validation
    ->
presentation
```

### Lesson

Every DAG used as a child workflow should have an explicit input
contract.

------------------------------------------------------------------------

## 13. Package registry warning

### Symptom

A temporary package registry connectivity warning appeared in Composer
logs.

### Resolution

No infrastructure change was made because the warning did not prevent
the final pipeline execution.

### Lesson

Classify warnings by impact before changing networking or environment
configuration.

------------------------------------------------------------------------

# Debugging Method Used

For each failure, the workflow was:

``` text
1. Read the task log
2. Identify the failing operator/function
3. Determine whether the failure is:
      - code
      - data
      - IAM
      - configuration
      - orchestration/resource
4. Change the smallest necessary component
5. Re-run the affected stage
6. Verify the output independently
7. Only then continue downstream
```

This is preferable to making multiple changes simultaneously because it
preserves causal visibility.


---
