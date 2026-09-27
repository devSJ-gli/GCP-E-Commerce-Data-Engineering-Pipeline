# GCP E-Commerce Data Engineering Pipeline — Measured Results

> **Evidence source:** measurements captured from the GCP environment used for the GCP E-Commerce Data Engineering Pipeline on 2026-09-26.
> Values below are recorded from the actual query and Cloud Shell outputs. No estimates are used.
>
> **Evidence convention:** screenshots referenced below are stored under `screenshots/BigQuery/`, `screenshots/GCS/`, or `screenshots/Evidence/`, depending on the evidence category.
> Airflow runtime remains explicitly unrecorded because a supporting runtime screenshot was not provided.

---

## 1. GCS Raw Layer

| Metric | Result |
|---|---|
| Bucket | `project-olist-data` |
| Prefix | `raw/ecom/` |
| Source file count | **9 CSV files** |
| Total raw size | **126,186,995 bytes (120.34 MiB)** |

The `gcloud storage ls -l` output showed 9 source CSV files. The command also reported directory/prefix objects, so the source-file count is based on the actual CSV files rather than the total object count.

**Command used:**

```bash
gcloud storage du -s gs://project-olist-data/raw/ecom/
gcloud storage ls -l gs://project-olist-data/raw/ecom/
```

**Measured output:**

``` 
TOTAL: 11 objects, 126186995 bytes (120.34MiB)
```

**Evidence:**

``` 
screenshots/Evidence/13_gcs_size.png
```

---

## 2. BigQuery Storage

`region-us.INFORMATION_SCHEMA.TABLE_STORAGE` could not be queried with the current identity because it lacked `bigquery.tables.getData`.

The same measurements were therefore collected successfully with `bq show`, using the table metadata fields `numTotalLogicalBytes` and `numTotalPhysicalBytes`.

### Dataset summary

| Dataset | Logical MB | Physical MB |
|---|---:|---:|
| staging | 95.17 | 171.32 |
| curated | 102.53 | 241.23 |
| presentation | 28.21 | 25.86 |
| audit | 0.01 | 0.02 |
| **Total** | **225.93** | **438.43** |

**Evidence:**

``` 
screenshots/Evidence/12_storage_measurements.png
```

The storage output covered all 24 pipeline tables:

- staging: 9
- curated: 9
- presentation: 4
- audit: 2

---

## 3. Table Row Counts

### Staging

| Table | Rows |
|---|---:|
| `olist_customers_dataset` | 99,441 |
| `olist_geolocation_dataset` | 1,000,163 |
| `olist_order_items_dataset` | 112,650 |
| `olist_order_payments_dataset` | 103,886 |
| `olist_order_reviews_dataset` | 99,224 |
| `olist_orders_dataset` | 99,441 |
| `olist_products_dataset` | 32,951 |
| `olist_sellers_dataset` | 3,095 |
| `product_category_name_translation` | 71 |

**Evidence:**

``` 
screenshots/BigQuery/01_staging_tables.png
```

### Curated

| Table | Rows |
|---|---:|
| `customers` | 99,441 |
| `geolocation` | 1,000,163 |
| `order_items` | 112,650 |
| `orders` | 99,441 |
| `payments` | 103,886 |
| `product_category_translation` | 71 |
| `products` | 32,951 |
| `reviews` | 99,224 |
| `sellers` | 3,095 |

**Evidence:**

``` 
screenshots/BigQuery/02_curated_tables.png
```

### Presentation

| Table | Rows |
|---|---:|
| `customer_summary` | 99,441 |
| `delivery_summary` | 99,441 |
| `product_summary` | 32,951 |
| `sales_summary` | 111 |

**Evidence:**

``` 
screenshots/BigQuery/03_presentation_tables.png
```

---

## 4. Validation Results

The latest shared pipeline run produced:

| Metric | Staging | Curated |
|---|---:|---:|
| Validation records | 9 | 9 |
| PASS records | 9 | 9 |
| NULL violations | 0 | 0 |
| Duplicate violations | 0 | 0 |
| Range violations | N/A | 0 |

All 9 staging validations and all 9 curated validations passed for the current run.

**Pipeline run ID:**

``` 
manual__2026-09-26T14:50:21.327486+00:00
```

**Evidence:**

``` 
screenshots/Evidence/04_audit_validation.png
screenshots/Evidence/05_curated_validation.png
```

---

## 5. Transformation Audit

The latest transformation audit contains 9 records, all with `PASS` status and matching source/target row counts.

| Source table | Target table | Source rows | Target rows | Status |
|---|---|---:|---:|---|
| `olist_customers_dataset` | `customers` | 99,441 | 99,441 | PASS |
| `olist_geolocation_dataset` | `geolocation` | 1,000,163 | 1,000,163 | PASS |
| `olist_order_items_dataset` | `order_items` | 112,650 | 112,650 | PASS |
| `olist_orders_dataset` | `orders` | 99,441 | 99,441 | PASS |
| `olist_order_payments_dataset` | `payments` | 103,886 | 103,886 | PASS |
| `product_category_name_translation` | `product_category_translation` | 71 | 71 | PASS |
| `olist_products_dataset` | `products` | 32,951 | 32,951 | PASS |
| `olist_order_reviews_dataset` | `reviews` | 99,224 | 99,224 | PASS |
| `olist_sellers_dataset` | `sellers` | 3,095 | 3,095 | PASS |

**Evidence:**

``` 
screenshots/Evidence/06_audit_transformation.png
```

---

## 6. Pipeline Run Correlation

### Verified current run

``` 
manual__2026-09-26T14:50:21.327486+00:00
```

For this run:

``` 
staging validation records   = 9
curated validation records   = 9
transformation records       = 9
passed transformations       = 9
```

The validation and transformation audit records therefore share the same current pipeline run ID.

**Evidence:**

``` 
screenshots/Evidence/07.1_run_id_correlation.png
screenshots/Evidence/07.2_run_id_correlation.png
```

> Note: the validation correlation query also returned an older row with a null `run_id`. That older result is not used for the documented current pipeline run. The current run above is the verified run used for the pipeline evidence.

---

## 7. Product Category Completeness

Measured result:

``` 
total_products = 32,951
products_without_category = 610
NULL category rate = 1.85%
```

**Evidence:**

``` 
screenshots/Evidence/08_category_completeness.png
```

The missing category values are retained as a measured source-data completeness result rather than being silently replaced.

---

## 8. Review Key Verification

The review logical key was verified as the composite:

``` 
(review_id, order_id)
```

Measured result:

``` 
duplicate_key_groups = 0
```

**Evidence:**

``` 
screenshots/Evidence/09_review_key_verification.png
```

---

## 9. Presentation Verification

The presentation row counts were verified as:

``` 
customer_summary = 99,441
delivery_summary = 99,441
product_summary  = 32,951
sales_summary    = 111
```

A direct content query against `presentation.sales_summary` returned populated records with sales metrics including order count, item count, product revenue, freight revenue, total revenue, and average order value.

**Evidence:**

``` 
screenshots/Evidence/10_presentation_content.png
```

---

## 10. BigQuery Job Measurements

The verification query returned the completed query jobs from the preceding 24 hours.

The largest `total_bytes_processed` value visible in the captured result was:

``` 
6,742,232 bytes
```

The displayed `duration_seconds` values were rounded to `0` for the captured jobs, so a meaningful largest-job duration was **not recorded** from this evidence.

Slot-ms was also **not captured** by the verification query.

| Measurement | Result |
|---|---:|
| Largest captured job by bytes processed | **6,742,232 bytes** |
| Largest captured job duration | **Not recorded; displayed as 0 seconds** |
| Total slot-ms for largest job | **Not captured** |

**Evidence:**

``` 
screenshots/Evidence/11_job_measurements.png
```

> These are BigQuery verification-job measurements. They should not be presented as the end-to-end Airflow pipeline runtime.

---

## 11. Airflow Runtime Measurements

Runtime measurements were intended to be captured from the Airflow UI rather than inferred from BigQuery query history.

No supporting Airflow runtime screenshot was included in the current evidence set, so these values are intentionally left as **Not captured** rather than estimated.

| DAG | Start | End | Runtime |
|---|---|---|---:|
| `gcs_to_bq` | Not captured | Not captured | Not captured |
| `validation` | Not captured | Not captured | Not captured |
| `transformation` | Not captured | Not captured | Not captured |
| `curated_validation` | Not captured | Not captured | Not captured |
| `presentation` | Not captured | Not captured | Not captured |
| `master_dag` | Not captured | Not captured | Not captured |

---

## 12. Final Measured Summary

| Metric | Result |
|---|---:|
| Source CSV files | **9** |
| Raw GCS size | **126,186,995 bytes (120.34 MiB)** |
| Staging rows | **1,545,922 total across 9 tables** |
| Curated rows | **1,545,922 total across 9 tables** |
| Presentation rows | **264,944 total across 4 tables** |
| Pipeline runtime | **Not captured** |
| Largest table by logical bytes | **`staging.olist_geolocation_dataset` — 40,555,093 bytes** |
| Largest table by physical bytes | **`curated.geolocation` — 88,689,162 bytes** |
| Largest captured BigQuery job | **6,742,232 bytes processed** |
| Validation | **18/18 validation records PASS** |
| Transformation | **9/9 transformations PASS** |
| Review composite-key duplicates | **0** |
| Products without category | **610 (1.85%)** |

### Evidence index

``` 
screenshots/BigQuery/01_staging_tables.png
screenshots/BigQuery/02_curated_tables.png
screenshots/BigQuery/03_presentation_tables.png
screenshots/Evidence/04_audit_validation.png
screenshots/Evidence/05_curated_validation.png
screenshots/Evidence/06_audit_transformation.png
screenshots/Evidence/07.1_run_id_correlation.png
screenshots/Evidence/07.2_run_id_correlation.png
screenshots/Evidence/08_category_completeness.png
screenshots/Evidence/09_review_key_verification.png
screenshots/Evidence/10_presentation_content.png
screenshots/Evidence/11_job_measurements.png
screenshots/Evidence/12_storage_measurements.png
screenshots/Evidence/13_gcs_size.png
```
