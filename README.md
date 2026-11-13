# Olist Retail Data Platform

A locally deployed, containerized data engineering platform built incrementally around the Olist retail dataset. The intended end state is a micro-batch ELT pipeline with ingestion, object storage, a PostgreSQL warehouse, dbt transformations, data-quality gates, an API, and monitoring.

> **Current milestone:** The incremental ingestion workflow through Gate 2 has been tested: source extraction, Parquet and manifest publication to S3 landing, Gate 1 validation and audit reporting, PostgreSQL `COPY` to raw tables, Gate 2 reconciliation, batch certification, watermark advancement, idempotent replay, and failure handling. **Next:** implement Gate 1 quarantine handling (copy failed artifacts, record reasons, mark batches `QUARANTINED`). Automated repair, tombstones, downstream quality gates, and production-ready analytics remain pending.

## Architecture — implemented components

```text
                         Airflow API server (UI :8080)
                                  |
                   +--------------+--------------+
                   |                             |
             Scheduler                     DAG processor
                   |                             |
                   +---------- Airflow metadata DB
                   |
              Valkey broker
                   |
          +--------+---------+
          |                  |
    2 general workers    1 batch worker
    default + ingest     batch queue
    concurrency 4 each   concurrency 2

Python Olist emulator ──► Source PostgreSQL (`source.shop`)
  (seed, replay, defects)      └─ `source.chaos` (ground-truth defect log)
            |
            v
  Airflow incremental extraction (`ingest` queue)
            |
            v
  Silo S3-compatible `landing/` (Parquet + manifest)
            |
            v
  Gate 1: landing validation + audit reporting
            | (valid batch)
            v
  PostgreSQL `COPY` ──► warehouse `raw` tables
            |
            v
  Gate 2: reconciliation ──► certification + watermark advancement

Warehouse PostgreSQL also hosts the separate `airflow` metadata database.
Silo endpoint :9000; console :9001; buckets: landing / quarantine / backups.
Quarantine bucket exists, but Gate 1 quarantine publication is pending.
```

All services are orchestrated with Docker Compose. `data-net` is an internal Docker network; `app-net` is used for components that need external connectivity. Database and broker ports are not published to the host. The Airflow UI and Silo ports are bound to `127.0.0.1`.

## Stack and status

| Component | Version / configuration | Status |
|---|---|---|
| Source PostgreSQL | `postgres:17.11-bookworm` | Running; basic connectivity checked |
| Warehouse PostgreSQL | `postgres:17.11-bookworm`, `max_connections=150` | Running; `airflow` database and `airflow_user` verified |
| S3-compatible object storage | `pgsty/silo:RELEASE.2026-10-26T00-00-00Z` | Running; three private, versioned buckets |
| S3 service identities | `pipeline_user`, `backup_user` | Write access and cross-bucket denial tested |
| Valkey | `valkey/valkey:8.1.11-alpine` | Running; password authentication and AOF enabled |
| Airflow | `apache/airflow:3.3.2-python3.12` | API server, scheduler and DAG processor healthy |
| CeleryExecutor | Two general workers + one batch worker | Three-queue smoke DAG passed |
| Triggerer | — | Not deployed; not needed for current DAGs |
| Olist source emulator | Python 3.12, psycopg, pandas, NumPy; Compose `emulator` profile | Implemented; one-week replay and deterministic restart tests passed |
| Incremental extraction and S3 landing | Airflow, Parquet + manifest | Implemented and tested |
| Gate 1 validation | Landing validation and audit reporting | Implemented and tested; quarantine action pending |
| Raw loading and Gate 2 | PostgreSQL `COPY`, reconciliation, certification | Implemented and tested |
| dbt transformations, later quality gates, API, dashboards | Later phases | Not included in validated milestone |

The infrastructure was configured interactively; not every acceptance test from the original plan has been completed. In particular, a clean-volume rebuild, comprehensive database role isolation, S3 read/delete-denial tests, and deployment automation should not be treated as verified solely from the smoke DAG.

## Repository layout

```text
.
├── airflow/
│   ├── dags/                      # Infrastructure smoke and ingestion DAGs
│   ├── include/                   # Shared extraction / validation / loading code
│   │   └── gates/                 # Gate 1 and Gate 2 helpers
│   └── plugins/
├── data/
├── docs/
│   ├── adr/
│   └── defect-catalog.md
├── infra/
│   ├── compose.yaml
│   ├── minio/policies/
│   ├── postgres/source-init/
│   └── postgres/warehouse-init/
├── scripts/
│   ├── create-db-roles.sh
│   ├── test-s3-access.py
│   └── check-phase2.sh
├── emulator/                     # Source emulator, SQL, defects config, tests
├── Makefile                      # Infrastructure and emulator shortcuts
├── dbt/                          # Configuration; downstream models not yet validated
├── quality/
├── api/
├── tests/
├── .env.example
└── README.md
```

The tree is a high-level guide, not an exhaustive listing of tracked files. Confirm that code and database migrations are committed before claiming reproducibility from a fresh clone.

## Requirements

- Linux/WSL2 with Docker Engine and Docker Compose plugin
- Python 3.12 and a local virtual environment for developer tooling
- Enough Docker resources to run PostgreSQL, Silo, Valkey, Airflow and three Celery workers
- `.env` created from `.env.example` with unique credentials (never commit `.env`)

## Run the current stack

From the repository root:

```bash
docker compose --env-file .env -f infra/compose.yaml config --quiet
docker compose --env-file .env -f infra/compose.yaml up -d

docker compose --env-file .env -f infra/compose.yaml ps
```

Interfaces:

- Airflow UI: <http://localhost:8080>
- Silo S3 endpoint: <http://localhost:9000>
- Silo console: <http://localhost:9001>

Airflow uses Simple Auth Manager with an `admin` account. **The generated password is sensitive.** In the current setup, the working password file was found at `/opt/airflow/simple_auth_manager_passwords.json.generated` inside the API-server container. Its persistence across container recreation has **not** been established; do not assume the admin password stays the same after recreation.

## Smoke test

The manually triggered `infrastructure_smoke_test` DAG contains three tasks:

1. `test_default_queue` → `default` queue → general worker
2. `test_ingest_queue` → `ingest` queue → general worker
3. `test_batch_queue` → `batch` queue → batch worker, after the first two

All three tasks succeeded after worker restart. This validates the core DAG parsing, scheduling, broker dispatch, Celery execution and task-state flow. To rerun, open the Airflow UI and manually trigger `infrastructure_smoke_test` (`schedule=None`).

## Object storage

| Bucket | Versioning | Noncurrent-version expiry | Service access |
|---|---|---|---|
| `landing` | Enabled | 30 days | `pipeline_user`: list/get/put |
| `quarantine` | Enabled | 30 days | `pipeline_user`: list/get/put |
| `backups` | Enabled | 30 days | `backup_user`: list/get/put |

Buckets are private. Neither policy grants object deletion. `PutObject` still permits overwriting an object key (which creates another version). S3 write-access isolation was tested successfully; a full authorization test suite is pending.

## Development practices

Phase 0 established Git, line-ending conventions, architectural decision records, and a defect catalog. The current pre-commit scope covers secret scanning (Gitleaks), LF line-ending normalization, whitespace/end-of-file cleanup, and YAML syntax/style checks. Python and SQL linting are not part of the current pre-commit configuration. Keep credentials in local `.env` files; commit only templates and non-secret policies.

## Roadmap

| Phase | Scope | Progress |
|---|---|---|
| 0 | Repository, tooling, ADRs, defect catalog | Substantially completed |
| 1 | PostgreSQL, object storage, broker, Airflow CeleryExecutor | Core deployment and smoke test completed; reproducibility/remaining acceptance items pending |
| 2 | Olist source database and emulator | One-week replay and restart tests reported passed; full-history acceptance pending |
| 3 | Incremental extraction to object storage | **Implemented and tested** through durable S3 landing |
| 4 | Ingestion quality gates and raw loading | **Partially implemented and tested:** Gate 1 checks, audit results, raw `COPY`, Gate 2 and certification; quarantine handling next |
| 5–6 | dbt staging and marts | Not started |
| 7–8 | Quality, reconciliation, defect injection | Not started |
| 9–10 | Serving API and observability | Not started |
| 11–12 | Security/backup/DR and CI/CD/documentation | Not started |

## Phase 2 — source emulator: tested milestone

The emulator replays historical Olist CSVs as changing operational records in `source.shop`, and records deliberately injected defects in `source.chaos`. Its Docker Compose service is activated with `--profile emulator`. The `Makefile` exposes `emulator-profile`, `emulator-migrate`, `emulator-seed`, `emulator-backfill`, `emulator-status`, `emulator-verify`, `emulator-fingerprint`, and `check2`.

The emulator replay, repeatability, and restart behavior were tested during Phase 2. Full-history validation remains pending. No row-count or event-count results are published here because the corresponding test output has not been independently verified for this documentation update.

To rerun the **destructive** local smoke test (resets emulator source data):

```bash
make check2
```

To inspect a seeded source without resetting it:

```bash
make emulator-status
make emulator-verify
```

## Phases 3–4 — validated ingestion milestone

The implemented workflow extracts changes from `source.shop`, publishes Parquet files and manifests to the S3-compatible `landing` bucket, validates batches in Gate 1, loads valid rows into warehouse `raw` tables using PostgreSQL `COPY`, and performs Gate 2 reconciliation before certifying batches and advancing the relevant watermark/checkpoint. Audit records capture validation and batch outcomes.

**Workflow checks reported as passed:**

| Check | Result |
|---|---|
| Extraction and S3 publication | Passed |
| Gate 1 validation and audit reporting | Passed |
| Raw load using PostgreSQL `COPY` | Passed |
| Gate 2 validation | Passed |
| Batch certification and watermark advancement | Passed |
| Idempotent replay and failure handling | Passed |

The checks above reflect the project handoff status, not independently attached test logs or full-history acceptance. In particular, a Gate 1 failure is not yet fully handled by copying its artifacts to quarantine and recording the `QUARANTINED` status. The successful-path tests do not establish that quarantine handling is complete.

### Next implementation priority: Gate 1 quarantine handling

For a failed Gate 1 batch:

1. Copy the failed Parquet and manifest artifacts into the `quarantine` bucket without deleting the original landing artifacts.
2. Persist structured failure reasons in the quarantine location and audit records.
3. Update the relevant batch status to `QUARANTINED` without advancing the certified checkpoint.
4. Test the failed-batch path, retries, and idempotent handling of repeated quarantine attempts.

**Deferred:** automatic repair, deletion tombstones, downstream staging/mart quality gates, reconciliation, and broader defect-injection coverage. The current milestone should not be described as a completed end-to-end analytics platform.
