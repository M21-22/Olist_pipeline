# Olist Retail Data Platform

A locally deployed, containerized data engineering platform built incrementally around the Olist retail dataset. The intended end state is a micro-batch ELT pipeline with ingestion, object storage, a PostgreSQL warehouse, dbt transformations, data-quality gates, an API, and monitoring.

> **Current milestone:** Phase 0 repository foundations and the core of Phase 1 infrastructure have been implemented. The Airflow/Celery smoke DAG completed successfully. This is **not yet** an operational retail ingestion or analytics pipeline.

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

Source PostgreSQL              Warehouse PostgreSQL
(source database)              (warehouse + separate airflow database)

Silo S3-compatible storage (:9000; console :9001)
  landing / quarantine / backups
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
| Olist emulator, extraction, dbt, quality gates, API, dashboards | Planned | Not implemented in current milestone |

The infrastructure was configured interactively; not every acceptance test from the original plan has been completed. In particular, a clean-volume rebuild, comprehensive database role isolation, S3 read/delete-denial tests, and deployment automation should not be treated as verified solely from the smoke DAG.

## Repository layout

```text
.
├── airflow/
│   ├── dags/infrastructure_smoke_test.py
│   └── plugins/
├── data/
├── docs/
│   ├── adr/
│   ├── defect-catalog.md
│   └── phase1-implementation-guide.md
├── infra/
│   ├── compose.yaml
│   ├── minio/policies/
│   ├── postgres/source-init/
│   └── postgres/warehouse-init/
├── scripts/
│   ├── create-db-roles.sh
│   └── test-s3-access.py
├── emulator/
├── dbt/
├── quality/
├── api/
├── tests/
├── .env.example
└── README.md
```

Some directories are placeholders for later phases. The database bootstrap files and scripts listed above reflect the working-session design; confirm they exist and are committed before treating the repository as a reproducible deployment.

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

Phase 0 established Git, `.gitignore`/`.gitattributes`, pre-commit checks (Ruff, SQLFluff, yamllint, Gitleaks and pre-commit-hooks), architectural decision records and the defect catalog. Keep credentials in ignored `.env` files; commit only templates and non-secret policies.

## Roadmap

| Phase | Scope | Progress |
|---|---|---|
| 0 | Repository, tooling, ADRs, defect catalog | Substantially completed |
| 1 | PostgreSQL, object storage, broker, Airflow CeleryExecutor | Core deployment and smoke test completed; reproducibility/remaining acceptance items pending |
| 2 | Olist source database and emulator | Not started |
| 3 | Incremental extraction to object storage | Not started |
| 4 | Ingestion quality gates and raw loading | Not started |
| 5–6 | dbt staging and marts | Not started |
| 7–8 | Quality, reconciliation, defect injection | Not started |
| 9–10 | Serving API and observability | Not started |
| 11–12 | Security/backup/DR and CI/CD/documentation | Not started |

For a chronological infrastructure setup guide, see [`docs/phase1-implementation-guide.md`](docs/phase1-implementation-guide.md).
