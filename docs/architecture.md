# Olist Retail Data Platform Architecture

```text
Emulator → source-db (PostgreSQL shop.*)
                  │
                  ▼
Airflow ingest (every 15 minutes; ingest queue)
  └─ Extract 7 mutable source tables in one REPEATABLE READ snapshot per table
       ├─ Explicit per-table column contract (include/common.py)
       ├─ landed watermark + 10-minute overlap; stable scheduled batch_id
       ├─ Server-side PostgreSQL cursor; fetch at most 10,000 records at once
       └─ Parquet (one file with bounded row groups), manifest statistics
                  │
                  ▼
S3-compatible storage: landing/<table>/dt=YYYY-MM-DD/hour=HH/<batch_id>.parquet
                       landing/<table>/dt=YYYY-MM-DD/hour=HH/<batch_id>.manifest.json
  └─ Upload Parquet → verify SHA-256 → publish manifest LAST
     → atomically register audit.landed_batches + extraction watermark
                  │
                  ▼
GATE 1 — include/gates/gate1.py (BEFORE raw COPY)
  ├─ Contract: identity and UTC-normalized logical interval
  ├─ Integrity: SHA-256, exact authoritative Arrow schema, fingerprint,
  │             scanned row count, NULL counts, control sums, min/max updated_at
  ├─ DuckDB: ONE whole-file uniqueness query over business key columns;
  │          bounded memory, disk spill directory
  └─ GX Core: ONE key/time DataFrame validation batch (key and updated_at
             non-NULL; updated_at > interval_start - overlap and <= interval_end)
       ├─ Individual PASS/FAIL checks → audit.dq_results
       └─ FAIL → audit.batch_registry FAILED; raw unchanged
           (S3 quarantine copies/alerts are pending)
                  │ PASS
                  ▼
raw warehouse (PostgreSQL)
  ├─ DELETE previous rows for same batch + COPY CSV (idempotent)
  ├─ GATE 2 — include/gates/gate2.py inside same transaction
  │    ├─ Actual raw row count vs validated manifest
  │    └─ Actual raw control sums vs validated manifest
  └─ PASS → certify batch + advance certified watermark in SAME transaction
     FAIL → roll back raw changes and certification
                  │
                  ▼
dbt staging (deduplication, normalization, latest-state views)
  └─ GATE 3 — dbt tests (including unique keys) + planned GX row-level rules
       └─ Invalid individual rows → quarantine.rejected_rows
                  │
                  ▼
dbt marts (facts, dimensions, purchase-time customer SCD2)
  ├─ Rebuild customer validity ranges from valid orders and customer records
  ├─ Resolve fact customer keys at the parent order purchase time
  └─ dbt marts validation; Gate 4 Soda freshness, volume and drift checks
                  │
                  ▼
6 API contract views (implemented) → FastAPI / Prometheus / Grafana (planned)

SEPARATE PERIODIC AUDIT — include/reconcile.py
  └─ Compare source state vs latest certified raw (missing, changed, deleted).
     Only reliable when source writes paused and certified ingest caught up.
```

## Transformation and validation

- **Airflow:** manual `transform_marts` runs staging validation, rejection synchronization, dbt marts and API validation, and the Gate 4 Soda scan on the `batch` queue. `transform_staging` runs staging validation and rejection synchronization independently.
- **Dimensions:** `dim_customer` (purchase-observed SCD2); `dim_product`, `dim_seller` (Type 1); `dim_geography` (representative ZIP-prefix geography); `dim_date` (calendar).
- **Facts:** `fct_orders`, `fct_order_items`, `fct_payments`, `fct_reviews` (incremental merge).
- **Analytics:** `mart_daily_kpis`, `mart_seller_delivery_sla`.
- **API schema:** `orders`, `order_items`, `seller_delivery_performance`, `sla_alerts`, `daily_kpis`, `categories_summary`. These are read-only SQL views for future consumers, not live HTTP endpoints.
- **Customer time semantics:** use `customer_unique_id` and order-associated geography observed at purchase timestamps. Sequential unchanged locations form one half-open SCD2 interval. Fact customer keys use the parent order's purchase timestamp. The model cannot infer unseen move dates. See [ADR-0009](adr/0009-rebuildable-customer-history.md).
- **Validation observed locally:** 70/70 isolated Python tests passed; dbt unit tests and all dbt data tests passed; `dbt docs generate` succeeded; `transform_marts` completed successfully.

## Operational state

- `audit.extraction_watermarks` and `audit.landed_batches` track landing.
- `audit.certified_watermarks` and `audit.batch_registry` track certification.
- `audit.dq_results` stores batch and transform check results. `audit.reconciliation` stores periodic drift audits.
- `audit.deleted_keys` holds observed source deletions until reconciliation certifies the key again.
- `audit.task_runs`, `audit.metrics_history`, `audit.serving_state` retained.
- Dropped tables: `audit.dbt_run_results` and `audit.api_log`.
- `chunk_ranges` is deliberately removed from the manifest and audit schema.

## Recovery and pending work

- Retry/replay **the same** batch ID can recover transient loader failures. A damaged
  already-published artifact has no automatic release path: require explicit manual
  remediation or a controlled re-extraction operation before later intervals can certify.
- Gate 1 quarantine artifacts are implemented and covered by local integration tests; automatic alert delivery and expanded defect-catalog evaluation remain pending.
- Staging includes complete certified reference-data tables and applies repair versions and tombstones for mutable tables.
- Warehouse quality gates and serving-state updates passed PostgreSQL fixture checks. Scheduled reconciliation can repair eligible source-to-raw differences. Full defect-catalog acceptance and clean CI verification remain pending.

See [plan.md](../plan.md), [customer-grain contract](customer-grain.md), [ADR](adr/) and [defect catalog](defect-catalog.md).
