-- GCP E-Commerce Data Engineering Pipeline - Verification Queries
-- Purpose: capture actual pipeline measurements and verification evidence.
-- Run BigQuery queries in US. Do not replace outputs with estimates.
--
-- GCS size commands are provided as comments because GCS object size is
-- not queried through BigQuery INFORMATION_SCHEMA.
--
-- Evidence captured for the current pipeline environment:
--   Pipeline run ID:
--   manual__2026-09-26T14:50:21.327486+00:00
--
-- IMPORTANT:
-- Each numbered section is intended to be run independently in BigQuery.
-- Save the resulting screenshots/output using the names documented in
-- docs/results/MEASUREMENTS.md.


-- ============================================================
-- 1. STAGING TABLE ROW COUNTS
-- ============================================================

SELECT 'olist_orders_dataset' AS table_name, COUNT(*) AS row_count
FROM `<GCP_PROJECT_ID>.staging.olist_orders_dataset`
UNION ALL
SELECT 'olist_order_items_dataset', COUNT(*)
FROM `<GCP_PROJECT_ID>.staging.olist_order_items_dataset`
UNION ALL
SELECT 'olist_order_payments_dataset', COUNT(*)
FROM `<GCP_PROJECT_ID>.staging.olist_order_payments_dataset`
UNION ALL
SELECT 'olist_products_dataset', COUNT(*)
FROM `<GCP_PROJECT_ID>.staging.olist_products_dataset`
UNION ALL
SELECT 'olist_customers_dataset', COUNT(*)
FROM `<GCP_PROJECT_ID>.staging.olist_customers_dataset`
UNION ALL
SELECT 'olist_sellers_dataset', COUNT(*)
FROM `<GCP_PROJECT_ID>.staging.olist_sellers_dataset`
UNION ALL
SELECT 'olist_order_reviews_dataset', COUNT(*)
FROM `<GCP_PROJECT_ID>.staging.olist_order_reviews_dataset`
UNION ALL
SELECT 'olist_geolocation_dataset', COUNT(*)
FROM `<GCP_PROJECT_ID>.staging.olist_geolocation_dataset`
UNION ALL
SELECT 'product_category_name_translation', COUNT(*)
FROM `<GCP_PROJECT_ID>.staging.product_category_name_translation`
ORDER BY table_name;


-- ============================================================
-- 2. CURATED TABLE ROW COUNTS
-- ============================================================

SELECT 'orders' AS table_name, COUNT(*) AS row_count
FROM `<GCP_PROJECT_ID>.curated.orders`
UNION ALL
SELECT 'order_items', COUNT(*)
FROM `<GCP_PROJECT_ID>.curated.order_items`
UNION ALL
SELECT 'payments', COUNT(*)
FROM `<GCP_PROJECT_ID>.curated.payments`
UNION ALL
SELECT 'products', COUNT(*)
FROM `<GCP_PROJECT_ID>.curated.products`
UNION ALL
SELECT 'customers', COUNT(*)
FROM `<GCP_PROJECT_ID>.curated.customers`
UNION ALL
SELECT 'sellers', COUNT(*)
FROM `<GCP_PROJECT_ID>.curated.sellers`
UNION ALL
SELECT 'reviews', COUNT(*)
FROM `<GCP_PROJECT_ID>.curated.reviews`
UNION ALL
SELECT 'geolocation', COUNT(*)
FROM `<GCP_PROJECT_ID>.curated.geolocation`
UNION ALL
SELECT 'product_category_translation', COUNT(*)
FROM `<GCP_PROJECT_ID>.curated.product_category_translation`
ORDER BY table_name;


-- ============================================================
-- 3. PRESENTATION TABLE ROW COUNTS
-- ============================================================

SELECT 'sales_summary' AS table_name, COUNT(*) AS row_count
FROM `<GCP_PROJECT_ID>.presentation.sales_summary`
UNION ALL
SELECT 'customer_summary', COUNT(*)
FROM `<GCP_PROJECT_ID>.presentation.customer_summary`
UNION ALL
SELECT 'product_summary', COUNT(*)
FROM `<GCP_PROJECT_ID>.presentation.product_summary`
UNION ALL
SELECT 'delivery_summary', COUNT(*)
FROM `<GCP_PROJECT_ID>.presentation.delivery_summary`
ORDER BY table_name;


-- ============================================================
-- 4. BIGQUERY TABLE STORAGE
-- ============================================================
--
-- Preferred query:
-- Run in the project that owns the datasets.
--
-- NOTE:
-- The current user identity did not have bigquery.tables.getData
-- permission for region-us.INFORMATION_SCHEMA.TABLE_STORAGE.
-- Therefore, the documented pipeline storage measurements were
-- collected with:
--
--   bq show --format=prettyjson PROJECT:DATASET.TABLE
--
-- and the fields:
--   numRows
--   numTotalLogicalBytes
--   numTotalPhysicalBytes
--
-- The following query remains useful when the required permission
-- is available.

SELECT
  table_schema,
  table_name,
  total_rows,
  ROUND(total_logical_bytes / POW(1024, 2), 2) AS logical_mb,
  ROUND(total_physical_bytes / POW(1024, 2), 2) AS physical_mb
FROM `region-us`.INFORMATION_SCHEMA.TABLE_STORAGE
WHERE table_schema IN ('staging', 'curated', 'presentation', 'audit')
ORDER BY total_logical_bytes DESC;


-- ============================================================
-- 5. VALIDATION AUDIT - LATEST NON-NULL RUN
-- ============================================================

WITH latest_run AS (
  SELECT run_id
  FROM `<GCP_PROJECT_ID>.audit.validation_results`
  WHERE run_id IS NOT NULL
  GROUP BY run_id
  ORDER BY MAX(validation_timestamp) DESC
  LIMIT 1
)
SELECT
  run_id,
  validation_layer,
  table_name,
  status,
  row_count,
  null_count,
  duplicate_count,
  range_violation_count,
  validation_timestamp
FROM `<GCP_PROJECT_ID>.audit.validation_results`
WHERE run_id = (SELECT run_id FROM latest_run)
ORDER BY validation_layer, table_name;


-- ============================================================
-- 6. TRANSFORMATION AUDIT - LATEST NON-NULL RUN
-- ============================================================

WITH latest_run AS (
  SELECT run_id
  FROM `<GCP_PROJECT_ID>.audit.transformation_results`
  WHERE run_id IS NOT NULL
  GROUP BY run_id
  ORDER BY MAX(transformation_timestamp) DESC
  LIMIT 1
)
SELECT
  run_id,
  source_table,
  target_table,
  source_row_count,
  target_row_count,
  status,
  transformation_timestamp
FROM `<GCP_PROJECT_ID>.audit.transformation_results`
WHERE run_id = (SELECT run_id FROM latest_run)
ORDER BY target_table;


-- ============================================================
-- 7. SHARED RUN-ID CHECK
-- ============================================================
--
-- Replace the value below only when verifying a different run.
-- Current verified pipeline run:
-- manual__2026-09-26T14:50:21.327486+00:00

WITH validation AS (
  SELECT
    COUNT(*) AS validation_records,
    COUNTIF(validation_layer = 'staging') AS staging_validation_records,
    COUNTIF(validation_layer = 'curated') AS curated_validation_records
  FROM `<GCP_PROJECT_ID>.audit.validation_results`
  WHERE run_id = 'manual__2026-09-26T14:50:21.327486+00:00'
),
transformation AS (
  SELECT
    COUNT(*) AS transformation_records,
    COUNTIF(status = 'PASS') AS passed_transformations
  FROM `<GCP_PROJECT_ID>.audit.transformation_results`
  WHERE run_id = 'manual__2026-09-26T14:50:21.327486+00:00'
)
SELECT
  'manual__2026-09-26T14:50:21.327486+00:00' AS run_id,
  validation.validation_records,
  validation.staging_validation_records,
  validation.curated_validation_records,
  transformation.transformation_records,
  transformation.passed_transformations
FROM validation
CROSS JOIN transformation;


-- ============================================================
-- 8. PRODUCT CATEGORY COMPLETENESS
-- ============================================================

SELECT
  COUNT(*) AS total_products,
  COUNTIF(product_category_name IS NULL) AS products_without_category,
  ROUND(
    SAFE_DIVIDE(
      COUNTIF(product_category_name IS NULL),
      COUNT(*)
    ) * 100,
    2
  ) AS null_category_rate_percent
FROM `<GCP_PROJECT_ID>.curated.products`;


-- ============================================================
-- 9. REVIEW KEY VERIFICATION
-- ============================================================

SELECT
  COUNT(*) AS duplicate_groups
FROM (
  SELECT
    review_id,
    order_id,
    COUNT(*) AS duplicate_count
  FROM `<GCP_PROJECT_ID>.curated.reviews`
  GROUP BY review_id, order_id
  HAVING COUNT(*) > 1
);


-- ============================================================
-- 10. PRESENTATION CONTENT CHECK
-- ============================================================
--
-- This verifies that the presentation layer contains populated
-- summary tables and that sales_summary exposes the expected
-- analytical measures.

SELECT
  'customer_summary' AS table_name,
  COUNT(*) AS row_count
FROM `<GCP_PROJECT_ID>.presentation.customer_summary`
UNION ALL
SELECT
  'delivery_summary',
  COUNT(*)
FROM `<GCP_PROJECT_ID>.presentation.delivery_summary`
UNION ALL
SELECT
  'product_summary',
  COUNT(*)
FROM `<GCP_PROJECT_ID>.presentation.product_summary`
UNION ALL
SELECT
  'sales_summary',
  COUNT(*)
FROM `<GCP_PROJECT_ID>.presentation.sales_summary`;

-- Optional content sample for sales_summary:
-- SELECT *
-- FROM `<GCP_PROJECT_ID>.presentation.sales_summary`
-- ORDER BY purchase_year, purchase_month
-- LIMIT 10;


-- ============================================================
-- 11. BIGQUERY JOB RUNTIME / BYTES PROCESSED
-- ============================================================
--
-- Run in the project that owns the jobs.
-- This captures completed query jobs from the last 7 days.
-- It is evidence about BigQuery jobs, not Airflow end-to-end
-- pipeline runtime.

SELECT
  creation_time,
  end_time,
  job_id,
  statement_type,
  TIMESTAMP_DIFF(end_time, creation_time, SECOND) AS duration_seconds,
  ROUND(total_bytes_processed / POW(1024, 3), 3) AS bytes_processed_gb,
  total_slot_ms,
  query
FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
  AND state = 'DONE'
  AND end_time IS NOT NULL
  AND statement_type IS NOT NULL
ORDER BY creation_time DESC
LIMIT 30;


-- ============================================================
-- 12. GCS RAW SIZE
-- ============================================================
--
-- Run from Cloud Shell / local gcloud CLI, not BigQuery:
--
-- gcloud storage du -s gs://project-olist-data/raw/ecom/
--
-- For individual source files:
--
-- gcloud storage ls -l gs://project-olist-data/raw/ecom/
--
-- Current measured result:
--   126186995 bytes (120.34 MiB)
--
-- Record the measured result in:
--   docs/results/MEASUREMENTS.md
-- ============================================================
