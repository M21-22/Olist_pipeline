# Olist Retail Data Platform

A containerized data engineering platform based on the [Olist Brazilian E-Commerce dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce). The project is designed as an end-to-end **micro-batch ELT platform**, progressing from simulated operational data through auditable ingestion, validation, warehousing, analytics, serving, and observability.

> **Project status:** Phase 7 passed local acceptance. Gate 3 validates staging, Gate 4 records the warehouse serving decision, and daily reconciliation repairs eligible differences between the source and certified raw data.

## Architecture

```text
Olist dataset
    │
    ▼
Python source emulator ──► PostgreSQL source (shop)
    │                           │
    └─ defect ground truth      │ updated_at-based extraction
       (chaos)                  ▼
                         Airflow ingestion DAG
                         (15-minute micro-batches)
                                │
                     Per-table extraction tasks
                                │
                                ▼
                      S3-compatible object store
                        landing/ (Parquet + manifest)
                                │
                                ▼
                      Gate 1: landing validation
                                │
                                ▼
                      Gate 2: transactional load
                                │
                                ▼
                     PostgreSQL warehouse
                        raw + audit schemas
                                │
                       ┌────────┴──────────────┐
                       ▼                       ▼
                 Reconciliation          dbt staging
                 (comparison modes)      incremental current state
                                               │
                                               ▼
                                         Gate 3: dbt tests
                                         + rejection routing
                                               │
                                               ▼
                                        dbt marts + API contract views
                                        purchase-time customer SCD2
                                        Gate 4/Soda checks
                                               │
                                               ▼
                                        API / monitoring
                                           (planned)

Orchestration: Airflow 3.3.2 + CeleryExecutor + Valkey
Runtime: Docker Compose  |  Source and warehouse: PostgreSQL 17
```

The platform runs locally with Docker Compose. The test environment uses a separate, disposable database stack.

## Technology stack

| Layer | Technologies | Status |
|---|---|---|
| Source simulation | Python, PostgreSQL, deterministic replay | Implemented and tested |
| Orchestration | Apache Airflow 3.3.2, CeleryExecutor, Valkey | Running locally |
| Ingestion | Python, incremental SQL extraction, Parquet | Implemented and tested |
| Object storage | Silo (S3-compatible), private landing/quarantine/backup buckets | Deployed; access isolation tested |
| Warehouse | PostgreSQL 17; raw, staging, marts, api, audit and quarantine schemas | Implemented for ingestion and transformations |
| Data validation | Great Expectations, PyArrow, DuckDB | Landing validation implemented |
| Data reconciliation | Source-to-warehouse comparison, watermarks | Continuous and full comparison modes implemented |
| Transformations | dbt Core 1.11.17, dbt-postgres 1.11.0 (isolated batch-worker environment) | Staging, purchase-time customer SCD2, dimensional/fact marts, analytics and API views built and tested locally |
| Serving and monitoring | FastAPI, Grafana, Prometheus | Planned |
| Automated testing | pytest, dbt unit/data tests, Docker Compose, GitHub Actions workflow | 70/70 local Python tests and all dbt tests passed; clean GitHub CI run unverified |

## Development progress

See [the implementation roadmap](plan.md) for target acceptance criteria.

| Scope | Progress |
|---|---|
| Architecture decisions, repository setup, development tooling | Foundation established |
| Docker infrastructure, PostgreSQL, object storage, Airflow and Celery | Core services operational; queue-dispatch smoke test passed |
| Olist source database and event emulator | One-week deterministic replay and restart-equivalence checks passed |
| Incremental extraction and Parquet landing | Implemented; timestamp-boundary, overlap, and runtime checks passed |
| Gate 1, Gate 2, transactional raw loading and certification | Implemented; unit and PostgreSQL integration checks passed |
| dbt staging and Gate 3 staging validation | Implemented and locally tested; batch DAG succeeded; broader release checks remain |
| dbt dimensional marts | **Local acceptance passed:** dimensional/fact/analytics models, API views, purchase-observed customer history, Python and dbt tests, dbt docs |
| Phase 7 — Gates 3–4 and reconciliation | **Local acceptance passed.** D08–D21 warehouse quality suite, reconciliation repair and dbt/Soda publication workflow |
| Defect injection and detection evaluation | Defect ground truth exists; comprehensive evaluation pending |
| Serving API | Planned |
| Metrics, monitoring, dashboards and alerts | Planned |
| Security, backup and disaster recovery | Initial access controls implemented; full acceptance pending |
| CI/CD, documentation, runbooks and delivery | Automated local regression suite passed; GitHub CI and final delivery pending |

### Source emulator

The emulator reproduces operational changes in the source database and records known injected defects separately. Its verified one-week scenario used **1–8 March 2017** with seed `42`.

- **3,502 events** replayed for **586 orders**.
- Repeatability and stop/resume equivalence checks passed.
- Source tables and defect records were verified after replay.

This demonstrates a reproducible test scenario, not a completed full-history simulation.

### Ingestion and raw certification

The `ingest` DAG runs every 15 minutes and uses dynamic task mapping to process seven mutable tables. Each table has its own `land → certify` dependency chain.

- **Incremental extraction:** `updated_at` boundaries with a configurable overlap, landing Parquet files and batch manifests.
- **Gate 1:** validates data and manifest contracts before warehouse loading; separates non-retryable data failures from retryable execution failures.
- **Gate 2:** performs transactional raw loading and certification; preserves rollback, audit evidence and certified watermark semantics on failure.
- **Operational checks:** DAG parsing, mapped task execution, empty batches and extraction boundaries have been tested.

Independent mapped execution has been observed. A **deliberate single-table failure-isolation test** is still pending.

### Reconciliation

The reconciliation implementation supports two modes:

- **Continuous:** compares eligible source records against certified raw data using the certified watermark; defers changes that cannot yet be evaluated reliably.
- **Full:** performs a stricter comparison when the source is quiescent and ingestion is caught up.

The scheduled `reconcile` DAG runs continuous comparison and repairs eligible differences for each mutable table. It re-extracts missing or changed keys in the same source snapshot, validates a targeted Parquet batch through Gates 1 and 2, and records observed deletions as tombstones. This does not advance the normal ingestion watermarks. A later eligible reconciliation can re-extract a reinserted key and clear its tombstone. Run `transform_marts` after reconciliation to apply the changes to staging and marts.

The batch-worker image includes reconciliation repair. A successful repair logs the original difference count and the repair batch ID in `audit.reconciliation`. Repair artifacts live under `landing/<table>/repair/`. The audit registry is authoritative for certification. A rollback can leave an unregistered object.

### dbt staging and Gate 3

The `transform_staging` DAG runs on the Airflow **`batch`** queue. Its tasks build current-state staging models, execute Gate 3 dbt validation, and synchronize rejected rows. The DAG was triggered manually and completed successfully.

- **Seven mutable sources** use certification-driven incremental `merge` with model-specific business keys. A durable processed-batch ledger (`audit.dbt_staging_processed_batches`) tracks consumed certifications, including late/tied certifications; `audit.dbt_staging_progress` provides progress reporting.
- **Two reference sources** (`geolocation` and `product_category_translation`) are materialized as tables from the latest complete certified reference-data batches.
- Staging deduplicates certified raw history to one current record per business key, and records `is_valid` and `source_batch_id`.
- **Gate 3** validates staging keys, provenance, allowed values, and relationships. Invalid current-state records remain visible with `is_valid = false` and are synchronized into `quarantine.rejected_rows`; downstream marts must exclude them.
- The isolated integration suite covers idempotency, late/tied certification, concurrent execution, failure recovery, full-refresh equivalence, and all seven mutable models. The staging test suite has also passed against the persistent warehouse.

Reconciliation repairs hard deletes and silent updates against certified raw when the keys are eligible under the overlap window. The next staging run applies certified repairs and tombstones.

### dbt marts and API views

The `transform_marts` DAG runs Gate 3 before it builds and validates dimensions, facts, analytical marts and API views. It can run manually or use the optional `OLIST_TRANSFORM_MARTS_SCHEDULE` cron. It then runs the Gate 4 Soda scan. Generic and singular data tests execute during `dbt build`. Fixture-based dbt unit tests run separately. The customer SCD2 timeline is built from purchase observations in staging.

- **Five dimensions:** `dim_customer` (purchase-observed SCD2), `dim_product` and `dim_seller` (Type 1), `dim_geography` (ZIP-prefix representative location), and `dim_date` (calendar).
- **Four incremental facts:** `fct_orders`, `fct_order_items`, `fct_payments`, and `fct_reviews`. The customer key is resolved at the parent order's purchase timestamp; late purchase observations can change historical customer ranges and cause fact re-keying.
- **Two analytical marts:** `mart_daily_kpis` and `mart_seller_delivery_sla`.
- **Six API contract views:** `orders`, `order_items`, `seller_delivery_performance`, `sla_alerts`, `daily_kpis`, and `categories_summary` in the `api` schema. These are database views, **not** implemented FastAPI endpoints.
- **Security boundary:** `api_reader` is intended to select from `api` only, not directly from staging or marts; the permission assertion is part of the dbt data tests.

`dim_customer` reconstructs change intervals from valid orders and their order-associated customer geography, grouped by `customer_unique_id`. Consecutive matching observations collapse into one half-open interval. It describes **geography observed at purchases**, not all real-world address changes. See [`docs/adr/0009-rebuildable-customer-history.md`](docs/adr/0009-rebuildable-customer-history.md).

**Verified locally:** the `transform_marts` DAG completed, the isolated Python suite passed **70/70**, dbt unit tests passed, dbt generic/singular data tests passed, and `dbt docs generate` succeeded. This does not establish end-to-end automated publishing or a clean GitHub Actions run.

See the [customer-grain contract](docs/customer-grain.md) for model grains and join rules.

### Automated testing

An isolated test environment exercises ingestion and quality logic without sharing the main warehouse's database volumes. The local `make test` run passed **70/70 tests**, covering Gate 1, Gate 2, PostgreSQL transaction behavior, extraction boundaries, reconciliation, DAG structure, dbt staging, Gate 3 rejection routing, incremental replay/recovery, and idempotency.

The GitHub Actions regression workflow is configured, but a successful clean-run result on GitHub has **not yet been confirmed**.

## Getting started

**Requirements:** Linux or WSL2, Docker Engine with Docker Compose, and a local `.env` populated from `.env.example`. Never commit credentials or `.env`.

From the repository root:

```bash
cp .env.example .env
# Set the required local passwords and settings in .env.

docker compose --env-file .env -f infra/compose.yaml config --quiet
docker compose --env-file .env -f infra/compose.yaml up -d
docker compose --env-file .env -f infra/compose.yaml ps
```

Local interfaces (bound to loopback in the current Compose setup):

| Service | URL |
|---|---|
| Airflow UI | http://localhost:8080 |
| Object-storage S3 endpoint | http://localhost:9000 |
| Object-storage console | http://localhost:9001 |

### Run staging validation

After the services are started, use the Airflow UI to trigger `transform_staging` manually. The DAG executes staging build → Gate 3 validation → rejection synchronization on the `batch` queue. It does not publish marts or API data.

### Run marts and API transformations

Run `ingest` first so the seven mutable tables have certified checkpoints. The batch worker needs the Soda runtime and `SODA_READER_PASSWORD`. Obtain a successful `reconcile` run for all seven mutable tables, then trigger `transform_marts`. For a manual reconciliation, set its logical date to a successful ingestion run’s logical date plus the `execution_delta` in [the reconciliation DAG](airflow/dags/reconcile.py).

The SQL in `infra/postgres/warehouse-init/` defines the schema, drift functions,
and grants for a fresh warehouse. Keep an existing warehouse that already has
these definitions. `make warehouse-rebuild` is a reset for a disposable local
warehouse. It drops and recreates the warehouse database before applying the SQL.

| Condition | Serving state |
|---|---|
| dbt and Soda checks pass | `HEALTHY` |
| dbt or Soda reports warnings, with all blocking checks passing | `DEGRADED` |
| Transform in progress, dbt failure, Soda failure, or scan error | `BLOCKED` |
| Certified input consumed by staging is older than 45 minutes | `DEGRADED` |
| Certified input is missing or older than 90 minutes | `BLOCKED` |
| Latest source-to-raw audit has unresolved eligible drift, or any input lacks reconciliation evidence from the past 30 hours | `BLOCKED` on the next publication attempt |

The freshness check uses certified window end times, so empty heartbeats advance freshness and historical Olist business dates do not make a current run stale. `audit.metrics_history` keeps observations from successful publications, including publications with Soda warnings. The rolling reference uses the first observation per certified window, includes only observations without Soda warnings, and evaluates after seven prior windows using up to 30. Order item and payment totals must agree with their component facts. Differences between item-plus-freight and payment amounts are profiled separately because the source may contain legitimate differences.

`audit.dq_results` keeps dbt and Soda check outcomes per transform attempt. `audit.serving_state` holds the publication decision. The future FastAPI service will enforce that decision for consumers of the API views. Gate 4 reads the latest result per table, including `DEFERRED`, and evaluates unresolved eligible keys independently of deferred keys. The 30-hour coverage limit allows six hours of delay for the daily reconciliation.

### Calibrate Soda on a clean replay

Use an isolated Olist stack for a fresh replay. Before seeding it, set
`EMULATOR_DEFECTS_ENABLED=false` in its `.env` and leave external injectors off.
The emulator includes defect settings in its replay fingerprint, so keep the
setting fixed from seed through replay. To collect data automatically, set
`OLIST_TRANSFORM_MARTS_SCHEDULE="5,20,35,50 * * * *"` before starting Airflow
and enable the `ingest`, `reconcile`, and `transform_marts` DAGs. This starts publication five
minutes after the corresponding ingestion trigger. Choose a capture period with
ordinary traffic, including quiet periods, and at least 28 distinct certified
windows per series. Allow extra windows if a series is missing values or has
too few accepted observations. Keep the emulator replaying during the capture.

Run the profile and apply commands with the UTC boundaries of your clean
publication period. The start is inclusive and the end is exclusive.

```bash
export CLEAN_FROM=2026-11-23T00:00:00Z CLEAN_UNTIL=2026-11-24T00:00:00Z
make profile-soda-drift
make apply-soda-drift
```

The profile fits a separate floor per metric and object using the 95th
percentile of historical absolute deviations. After seven warm-up windows, it
requires at least 14 scored fit windows and reserves the last seven for
validation. It reports the warning count under both the default and proposed
floor. `make apply-soda-drift` installs only `READY` thresholds. Other series keep
their current floor. Soda uses the larger of the floor and three sample
standard deviations. Use the capture bounds for the warehouse being calibrated,
since replay speed and publication cadence affect the observations.

In a local replay of the public Olist data with zero injected defects, 47
15-minute windows from 16 April 2018 followed the bootstrap window. The
emulator and compiled dbt models supplied the metrics in disposable PostgreSQL.
The profile installed 17 of 18 thresholds. The proposed
`mart_seller_delivery_sla` row-count floor increased validation warnings from
four to six, so that series kept its default. The observed order settlement
difference rate ranged from 0.198% to 0.238%. [Olist source dataset](https://www.kaggle.com/olistbr/brazilian-ecommerce/home/)

### Run the test suite

```bash
make test              # Full isolated unit + integration regression suite
make test-unit         # Unit tests
make test-integration  # PostgreSQL integration tests
make test-clean        # Remove disposable test services and volumes
make test-warehouse-quality  # Run the D08–D21 suite in a disposable project
make test-d12          # Run only the silent-update scenario
```

`make test-warehouse-quality` reports a representative case for each D08–D21 defect ID and handles fixture setup and cleanup. The volume and distribution cases use synthetic history to verify Soda's decisions, including the effect of a fitted floor. D20 checks the SQL API view, while HTTP response contracts will be tested with the serving API.

The test environment is defined in `infra/compose.test.yaml` and is independent of the running application database volumes.

dbt is installed **inside** `airflow-worker-batch`, not on the WSL host. To run unit tests, data tests and documentation generation:

```bash
docker compose --env-file .env -f infra/compose.yaml exec -T airflow-worker-batch bash -ec '
  export DBT_TARGET_PATH=/tmp/olist-dbt-target
  export DBT_LOG_PATH=/tmp/olist-dbt-logs
  DBT=/home/airflow/dbt-venv/bin/dbt
  ARGS="--project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt"
  $DBT test $ARGS --select test_type:unit
  $DBT test $ARGS --exclude test_type:unit
  $DBT docs generate $ARGS
'
```

The generated `index.html`, `manifest.json` and `catalog.json` reside in the container's `/tmp/olist-dbt-target`.

## Repository structure

```text
.
├── airflow/                 # DAGs, extraction, validation, loading, reconciliation
├── emulator/                # Operational source simulator and defect injection
├── infra/                   # Compose stacks, PostgreSQL setup, storage configuration
├── dbt/                     # Staging, dimensional marts, API contract views, macros and dbt tests
├── tests/                   # Unit and integration tests
├── scripts/                 # Utility, validation and smoke-test commands
├── docs/                    # Architecture, decisions, phase guides, project-state
├── .github/workflows/       # Regression CI workflow
├── Makefile                 # Common operational and test targets
└── plan.md                  # Full implementation roadmap and acceptance criteria
```

## Next milestones

1. Extend fault injection and measure detection coverage across the full defect catalog.
2. Confirm the regression workflow passes in GitHub Actions from a clean checkout.
3. Build the serving API, observability, security and backup validation, and final runbooks.

See [`plan.md`](plan.md) for the full target architecture and phase acceptance criteria, and `docs/` for technical implementation decisions.
