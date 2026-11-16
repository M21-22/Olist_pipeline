# Olist Retail Data Platform

A containerized data engineering platform based on the [Olist Brazilian E-Commerce dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce). The project is designed as an end-to-end **micro-batch ELT platform**, progressing from simulated operational data through auditable ingestion, validation, warehousing, analytics, serving, and observability.

> **Project status — 17 November 2026:** Infrastructure, the deterministic source emulator, incremental ingestion, and raw-layer certification are implemented and tested locally. Watermark-aware reconciliation and automated regression testing are also operational. **dbt transformations and downstream platform components remain in development.**

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
                       ┌────────┴─────────┐
                       ▼                  ▼
                   Reconciliation      dbt models
                   (implemented)       (in progress)
                                          │
                                          ▼
                                  Analytics / API /
                                  observability
                                     (planned)

Orchestration: Airflow 3.3.2 + CeleryExecutor + Valkey
Runtime: Docker Compose  |  Source and warehouse: PostgreSQL 17
```

The diagram distinguishes **implemented ingestion and reconciliation** from the **planned analytics and serving path**. The platform runs locally with Docker Compose; the test environment uses a separate, disposable database stack.

## Technology stack

| Layer | Technologies | Status |
|---|---|---|
| Source simulation | Python, PostgreSQL, deterministic replay | Implemented and tested |
| Orchestration | Apache Airflow 3.3.2, CeleryExecutor, Valkey | Running locally |
| Ingestion | Python, incremental SQL extraction, Parquet | Implemented and tested |
| Object storage | Silo (S3-compatible), private landing/quarantine/backup buckets | Deployed; access isolation tested |
| Warehouse | PostgreSQL 17, raw and audit schemas | Implemented for ingestion |
| Data validation | Great Expectations, PyArrow, DuckDB | Landing validation implemented |
| Data reconciliation | Source-to-warehouse comparison, watermarks | Continuous and full comparison modes implemented |
| Transformations | dbt | In progress |
| Serving and monitoring | FastAPI, Grafana, Prometheus | Planned |
| Automated testing | pytest, Docker Compose, GitHub Actions workflow | Local suite passed; GitHub CI verification pending |

## Development progress

The implementation follows the phased roadmap in [`plan.md`](plan.md). A phase is marked complete only to the extent supported by the checks described here; partially implemented features are not presented as finished.

| Phase | Scope | Progress |
|---|---|---|
| **0** | Architecture decisions, repository setup, development tooling | Foundation established |
| **1** | Docker infrastructure, PostgreSQL, object storage, Airflow and Celery | Core services operational; queue-dispatch smoke test passed |
| **2** | Olist source database and event emulator | One-week deterministic replay and restart-equivalence checks passed |
| **3** | Incremental extraction and Parquet landing | Implemented; timestamp-boundary, overlap, and runtime checks passed |
| **4** | Gate 1, Gate 2, transactional raw loading and certification | Implemented; unit and PostgreSQL integration checks passed |
| **5–6** | dbt staging and marts | In progress |
| **7** | Data quality and reconciliation | Initial continuous and full reconciliation implemented and tested; remaining gates pending |
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

### Automated testing

An isolated test environment exercises ingestion and quality logic without sharing the main warehouse's database volumes. The local `make test` run has passed, covering Gate 1, Gate 2, PostgreSQL transaction behavior, extraction boundaries, empty certification, reconciliation, DAG structure and function-call contracts.

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

### Run the test suite

```bash
make test              # Full isolated unit + integration regression suite
make test-unit         # Unit tests
make test-integration  # PostgreSQL integration tests
make test-clean        # Remove disposable test services and volumes
```

The test environment is defined in `infra/compose.test.yaml` and is independent of the running application database volumes.

## Repository structure

```text
.
├── airflow/                 # DAGs, extraction, validation, loading, reconciliation
├── emulator/                # Operational source simulator and defect injection
├── infra/                   # Compose stacks, PostgreSQL setup, storage configuration
├── dbt/                     # Analytics transformations (in development)
├── tests/                   # Unit and integration tests
├── scripts/                 # Utility, validation and smoke-test commands
├── docs/                    # Architecture, decisions, phase guides, project-state
├── .github/workflows/       # Regression CI workflow
├── Makefile                 # Common operational and test targets
└── plan.md                  # Full implementation roadmap and acceptance criteria
```

## Next milestones

1. Complete dbt staging and marts, including transformation and model-level quality checks.
2. Extend data-quality gates and complete controlled defect-detection evaluation.
3. Verify per-table failure isolation under a deliberately injected fault.
4. Confirm the regression workflow passes in GitHub Actions from a clean checkout.
5. Build the serving API, observability, security/backup validation and final runbooks.

See [`plan.md`](plan.md) for the full target architecture and phase acceptance criteria, and `docs/` for technical implementation decisions.
