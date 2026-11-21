# Olist Retail Data Platform

A containerized data engineering platform based on the [Olist Brazilian E-Commerce dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce). The project is designed as an end-to-end **micro-batch ELT platform**, progressing from simulated operational data through auditable ingestion, validation, warehousing, analytics, serving, and observability.

> **Project status — 21 November 2026:** Phases 0–6 have been implemented to the locally validated scope. Both `transform_staging` and `transform_marts` have succeeded; the isolated Python suite passed **70/70 tests**, dbt unit and warehouse data tests passed, and `dbt docs generate` succeeded. Customer history is reconstructed from purchase observations. **Gate 4/Soda, automated reconciliation repairs, publishing controls, and the serving/monitoring services remain future work.**

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
                                        Gate 4/Soda checks planned
                                               │
                                               ▼
                                        API / monitoring
                                           (planned)

Orchestration: Airflow 3.3.2 + CeleryExecutor + Valkey
Runtime: Docker Compose  |  Source and warehouse: PostgreSQL 17
```

The diagram separates **implemented ingestion, staging validation, reconciliation comparison, and dbt marts** from **planned Gate 4, automated repair, and serving**. The platform runs locally with Docker Compose; the test environment uses a separate, disposable database stack.

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

The implementation follows the phased roadmap in [`plan.md`](plan.md). A phase is marked complete only to the extent supported by the checks described here; partially implemented features are not presented as finished.

| Phase | Scope | Progress |
|---|---|---|
| **0** | Architecture decisions, repository setup, development tooling | Foundation established |
| **1** | Docker infrastructure, PostgreSQL, object storage, Airflow and Celery | Core services operational; queue-dispatch smoke test passed |
| **2** | Olist source database and event emulator | One-week deterministic replay and restart-equivalence checks passed |
| **3** | Incremental extraction and Parquet landing | Implemented; timestamp-boundary, overlap, and runtime checks passed |
| **4** | Gate 1, Gate 2, transactional raw loading and certification | Implemented; unit and PostgreSQL integration checks passed |
| **5** | dbt staging and Gate 3 staging validation | Implemented and locally tested; batch DAG succeeded; complete phase acceptance remains subject to broader release checks |
| **6** | dbt dimensional marts | **Local acceptance passed:** dimensional/fact/analytics models, API views, purchase-observed customer history, Python and dbt tests, dbt docs |
| **7** | Gate 4, broader quality checks, reconciliation repair | Continuous/full reconciliation comparison implemented; repair/tombstone generation and Soda checks pending |
| **8** | Defect injection and detection evaluation | Defect ground truth exists; comprehensive evaluation pending |
| **9** | Serving API | Planned |
| **10** | Metrics, monitoring, dashboards and alerts | Planned |
| **11** | Security, backup and disaster recovery | Initial access controls implemented; full acceptance pending |
| **12** | CI/CD, documentation, runbooks and delivery | Automated local regression suite passed; GitHub CI and final delivery pending |

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

A local continuous check of `orders` reported **586 matched records, 0 deferred, `MATCH`**. Automated repair and deletion/tombstone workflows are not yet complete.

### dbt staging and Gate 3

The `transform_staging` DAG runs on the Airflow **`batch`** queue. Its tasks build current-state staging models, execute Gate 3 dbt validation, and synchronize rejected rows. The DAG was triggered manually and completed successfully.

- **Seven mutable sources** use certification-driven incremental `merge` with model-specific business keys. A durable processed-batch ledger (`audit.dbt_staging_processed_batches`) tracks consumed certifications, including late/tied certifications; `audit.dbt_staging_progress` provides progress reporting.
- **Two reference sources** (`geolocation` and `product_category_translation`) are materialized as tables from the latest complete certified reference-data batches.
- Staging deduplicates certified raw history to one current record per business key, and records `is_valid` and `source_batch_id`.
- **Gate 3** validates staging keys, provenance, allowed values, and relationships. Invalid current-state records remain visible with `is_valid = false` and are synchronized into `quarantine.rejected_rows`; downstream marts must exclude them.
- The isolated integration suite covers idempotency, late/tied certification, concurrent execution, failure recovery, full-refresh equivalence, and all seven mutable models. The staging test suite has also passed against the persistent warehouse.

**Boundary:** reconciliation compares the source and certified raw state, but **automatic hard-delete detection, repair/tombstone creation, and reinsertion recovery are planned for Phase 7**. The staging layer can apply already registered deletion keys; this does not make automated repair complete.

### dbt marts, API views and Phase 6 acceptance

The `transform_marts` DAG runs manually on the Airflow `batch` queue **after** a successful `transform_staging` run. It builds dimensions, facts, analytical marts and API views, then executes singular data assertions. Generic model tests execute during `dbt build`; fixture-based dbt unit tests run separately. The customer SCD2 timeline is built from purchase observations in staging.

- **Five dimensions:** `dim_customer` (purchase-observed SCD2), `dim_product` and `dim_seller` (Type 1), `dim_geography` (ZIP-prefix representative location), and `dim_date` (calendar).
- **Four incremental facts:** `fct_orders`, `fct_order_items`, `fct_payments`, and `fct_reviews`. The customer key is resolved at the parent order's purchase timestamp; late purchase observations can change historical customer ranges and cause fact re-keying.
- **Two analytical marts:** `mart_daily_kpis` and `mart_seller_delivery_sla`.
- **Six API contract views:** `orders`, `order_items`, `seller_delivery_performance`, `sla_alerts`, `daily_kpis`, and `categories_summary` in the `api` schema. These are database views, **not** implemented FastAPI endpoints.
- **Security boundary:** `api_reader` is intended to select from `api` only, not directly from staging or marts; the permission assertion is part of the dbt data tests.

`dim_customer` reconstructs change intervals from valid orders and their order-associated customer geography, grouped by `customer_unique_id`. Consecutive matching observations collapse into one half-open interval. It describes **geography observed at purchases**, not all real-world address changes. See [`docs/adr/0009-rebuildable-customer-history.md`](docs/adr/0009-rebuildable-customer-history.md).

**Verified locally:** the `transform_marts` DAG completed, the isolated Python suite passed **70/70**, dbt unit tests passed, dbt generic/singular data tests passed, and `dbt docs generate` succeeded. This does not establish end-to-end automated publishing or a clean GitHub Actions run.

Implementation and acceptance details: [`docs/phase6-implementation-guide.md`](docs/phase6-implementation-guide.md).

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

After `transform_staging` succeeds, manually trigger `transform_marts` in Airflow. It runs `build_marts_and_api` followed by `validate_marts_and_api`. Publication gates and automatic staging-to-marts triggering are not part of Phase 6.

### Run the test suite

```bash
make test              # Full isolated unit + integration regression suite
make test-unit         # Unit tests
make test-integration  # PostgreSQL integration tests
make test-clean        # Remove disposable test services and volumes
```

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

The generated `index.html`, `manifest.json` and `catalog.json` reside in the container's `/tmp/olist-dbt-target`; they can be copied to WSL with `docker cp`. These are generated artifacts, not required source files to commit.

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

1. Complete Gate 4/Soda integration, automated reconciliation repair, and tombstone lifecycle management (Phase 7).
2. Verify per-table failure isolation under a deliberately injected fault and the full defect-detection matrix.
3. Confirm the regression workflow passes in GitHub Actions from a clean checkout.
4. Build the serving API, observability, security/backup validation, and final runbooks.

See [`plan.md`](plan.md) for the full target architecture and phase acceptance criteria, and `docs/` for technical implementation decisions.
