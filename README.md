# GCP E-Commerce Data Engineering Pipeline

End-to-end **batch** data engineering pipeline on Google Cloud Platform, built
around Cloud Storage, BigQuery, and Apache Airflow / Cloud Composer, using the
[Olist Brazilian E-Commerce dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce).

```
Source CSVs → Cloud Storage (Raw) → BigQuery L1 Staging → Validation
            → BigQuery L2 Curated → Validation
            → BigQuery L3 Presentation (BI-ready)
```

Airflow orchestrates the pipeline end to end; every layer transition is
validated and audited so any run can be traced back to source/target row
counts and pass/fail status without digging through logs.

## Highlights

- **Layered warehouse design** — raw → staging → curated → presentation, each
  with its own validation gate (NULL checks, duplicate keys, range checks).
- **Config-driven validation** — table-level rules live in YAML, not hardcoded
  per DAG.
- **Audit trail in BigQuery** — every validation and transformation run is
  logged to `audit.*` tables, correlated by a shared `pipeline_run_id` passed
  from a master DAG via `TriggerDagRunOperator`.
- **Dynamic task mapping** — ingestion and validation tasks expand over the
  table list at runtime rather than being hand-written per table.
- **4 presentation tables** (`sales_summary`, `customer_summary`,
  `product_summary`, `delivery_summary`) verified end to end: 18/18 validation
  records passed, 9/9 transformation audits, all passing on the final master run.

## Repository structure

```
├── README.md                    ← you are here
├── docs/
│   ├── PROJECT_DOCUMENTATION.md ← full architecture, DAG-by-DAG design notes
│   ├── TROUBLESHOOTING.md       ← real failures hit during dev + root cause + fix
│   ├── VERIFICATION_QUERIES.sql ← queries used to verify the final run
│   └── results/MEASUREMENTS.md  ← row counts, storage sizes, job stats
├── dags/                        ← Airflow DAGs + everything they load at
│   │                               runtime (Composer only syncs dags/)
│   ├── staging_ingestion.py
│   ├── validation.py
│   ├── transformation.py
│   ├── curated_validation.py
│   ├── presentation.py
│   ├── master_dag.py
│   ├── config/table_config.yaml ← validation rules per table
│   └── sql/
│       ├── transformations/     ← staging → curated SQL, one file per table
│       └── presentation/        ← curated → presentation SQL
└── screenshots/                 ← architecture and successful execution evidence
```

## Read more

- **Full design writeup:** [`docs/PROJECT_DOCUMENTATION.md`](docs/PROJECT_DOCUMENTATION.md)
- **What actually broke, and how it was fixed:** [`docs/TROUBLESHOOTING.md`](docs/TROUBLESHOOTING.md)
- **Verification results:** [`docs/results/MEASUREMENTS.md`](docs/results/MEASUREMENTS.md)

## Scope and known limitations

This is a **batch** pipeline that ends at presentation-ready BigQuery tables:

- Ingestion (`gcs_to_bq`) is a separately triggerable DAG — it takes
  `bucket`/`folder` as DAG params, so the master intentionally doesn't chain
  it (see the ingestion note in `docs/PROJECT_DOCUMENTATION.md`).
- Ingestion uses `WRITE_TRUNCATE` (full refresh, not incremental).
- Presentation models don't yet write their own audit records.
- No BI layer — Looker was intentionally left out; the presentation tables
  are BI-ready but consumption is out of scope for this project.

## Tech stack

| Area | Technology |
|---|---|
| Cloud | Google Cloud Platform |
| Object storage | Cloud Storage |
| Data warehouse | BigQuery |
| Orchestration | Apache Airflow / Cloud Composer |
| Transformation | BigQuery SQL |
| Validation | BigQuery SQL + Airflow Python tasks |
| Config | YAML |
| Language | Python |

---

License: MIT (see [`LICENSE`](LICENSE)).
