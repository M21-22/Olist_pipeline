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
dbt marts (facts, dimensions, customer SCD2 later)
  └─ GATE 4 — dbt tests + planned Soda freshness/volume/drift checks
                  │
                  ▼
API contract views → FastAPI → Prometheus/Grafana

SEPARATE PERIODIC AUDIT — include/reconcile.py
  └─ Compare source state vs latest certified raw (missing, changed, deleted).
     Only reliable when source writes paused and certified ingest caught up.
```

## Operational state

- `audit.extraction_watermarks` and `audit.landed_batches` track landing.
- `audit.certified_watermarks` and `audit.batch_registry` track certification.
- `audit.dq_results` stores Gate 1 results; `audit.reconciliation` stores periodic drift audits.
- `audit.deleted_keys` is reserved for future hard-delete repair.
- `audit.task_runs`, `audit.metrics_history`, `audit.serving_state` retained.
- Dropped tables: `audit.dbt_run_results` and `audit.api_log`.
- `chunk_ranges` is deliberately removed from the manifest and audit schema.

## Recovery and pending work

- Retry/replay **the same** batch ID can recover transient loader failures. A damaged
  already-published artifact has no automatic release path: require explicit manual
  remediation or a controlled re-extraction operation before later intervals can certify.
- Gate 1 quarantine/alerts and expanded defect-catalog rules are pending.
- Static reference snapshots, automated hard-delete repairs/tombstones are deferred.
- Full database/Docker integration testing is required before deploying these changes.

See [plan.md](../plan.md), [ADR](adr/) and [defect catalog](defect-catalog.md).
