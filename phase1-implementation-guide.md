# Phase 1 — Infrastructure Implementation Guide

This guide records the **correct sequence of actions** used to build the current local infrastructure. It deliberately excludes troubleshooting history. It is a guide to the implemented milestone, **not a claim that every acceptance criterion from the original Phase 1 plan has been completed**.

**Repository root:** `/home/murad/Projects_wsl/Olist_pipeline`
**Compose file:** `infra/compose.yaml`
**Compose project:** `olist-platform`

## 1. Prepare the repository and developer tooling

Create the repository directories (`infra/postgres/source-init`, `infra/postgres/warehouse-init`, `infra/minio/policies`, `airflow/dags`, `airflow/plugins`, `scripts`, `docs/adr`, `data`, `dbt`, `quality`, `api`, `tests`). Initialize Git, `.gitignore`, `.gitattributes`, a Python virtual environment and pre-commit. Configure Ruff, SQLFluff, yamllint, Gitleaks and standard pre-commit hooks. Record architectural decisions and maintain `docs/defect-catalog.md`.

Do not commit `.env`, local virtual environments, runtime logs or generated data.

## 2. Create credentials

Create `.env` from `.env.example`. Generate separate random passwords for the source and warehouse PostgreSQL administrators, Airflow metadata role, Valkey, Silo root account, and S3 service identities:

```bash
cp .env.example .env
openssl rand -hex 24
```

Run the password-generation command separately for each secret and populate `.env`. The working setup uses at least these variable names:

```dotenv
SOURCE_POSTGRES_PASSWORD=
WAREHOUSE_POSTGRES_PASSWORD=
AIRFLOW_DB_PASSWORD=
VALKEY_PASSWORD=
MINIO_ROOT_USER=
MINIO_ROOT_PASSWORD=
S3_PIPELINE_ACCESS_KEY=
S3_PIPELINE_SECRET_KEY=
S3_BACKUP_ACCESS_KEY=
S3_BACKUP_SECRET_KEY=
AIRFLOW_FERNET_KEY=
AIRFLOW_JWT_SECRET=
```

Additional database role passwords belong in `.env` as required by your actual SQL/bootstrap scripts. Set file permissions:

```bash
chmod 600 .env
```

Generate the Airflow secrets using Python (install `cryptography` in the local virtual environment if needed):

```python
import secrets
from cryptography.fernet import Fernet

print('AIRFLOW_FERNET_KEY=' + Fernet.generate_key().decode())
print('AIRFLOW_JWT_SECRET=' + secrets.token_hex(32))
```

Keep the Fernet key stable: changing it without key rotation can make previously encrypted Airflow values unreadable.

## 3. Configure Docker Compose foundation

At the top of `infra/compose.yaml`:

```yaml
name: olist-platform

services:
  # Services are defined in the following steps.

networks:
  data-net:
    internal: true
  app-net:

volumes:
  source_pgdata:
  warehouse_pgdata:
  valkey_data:
  object_store_data:
  airflow_logs:
  airflow_config:
```

Keep `data-net` internal. Publish only user-facing interfaces, bound to `127.0.0.1`. Add restart policies, log rotation and healthchecks to long-running services. Use pinned image versions.

## 4. Deploy PostgreSQL

Create two services in `infra/compose.yaml`:

| Service | Image | Database | Data volume | Network |
|---|---|---|---|---|
| `source-db` | `postgres:17.11-bookworm` | `source` | `source_pgdata:/var/lib/postgresql/data` | `data-net` |
| `warehouse-db` | `postgres:17.11-bookworm` | `warehouse` | `warehouse_pgdata:/var/lib/postgresql/data` | `data-net` |

Set `POSTGRES_USER=postgres` and `POSTGRES_PASSWORD` from the appropriate `.env` variable. Mount `./postgres/source-init:/docker-entrypoint-initdb.d:ro` and `./postgres/warehouse-init:/docker-entrypoint-initdb.d:ro` respectively. Configure `pg_isready` healthchecks. Set `max_connections=150` for the warehouse PostgreSQL server. Do **not** publish PostgreSQL ports.

The warehouse PostgreSQL instance also hosts a separate `airflow` metadata database. The verified metadata role/database configuration is:

```sql
-- Run on the warehouse PostgreSQL instance, connected to the postgres DB.
-- Supply the password securely rather than hardcoding it in committed SQL.
CREATE ROLE airflow_user LOGIN PASSWORD '<AIRFLOW_DB_PASSWORD>' NOSUPERUSER;
CREATE DATABASE airflow OWNER airflow_user;
```

These `CREATE` statements are intended for an **initially unconfigured** instance. For the actual repository, use your existing role-creation/bootstrap script and non-secret SQL grants; avoid duplicating roles/databases. The warehouse schemas created in the working session were `raw`, `staging`, `marts`, `audit`, `quarantine`, `api`. Other application roles included `source_reader`, `emulator_writer`, `etl_loader`, `dbt_runner`, `soda_reader`, `api_reader`, `grafana_reader`.

**Reproducibility note:** PostgreSQL scripts in `/docker-entrypoint-initdb.d/` run only when the data volume is empty. The current setup also involved manual post-initialization SQL. Before claiming a one-command fresh install, consolidate the role/database/schema creation into reliable first-boot scripts; do not delete populated volumes just to rerun initialization.

Start the database services:

```bash
docker compose --env-file .env -f infra/compose.yaml up -d source-db warehouse-db
```

## 5. Deploy Silo object storage

Define service `minio` (service name retained for S3 compatibility):

```yaml
  minio:
    image: pgsty/silo:RELEASE.2026-10-26T00-00-00Z
    restart: unless-stopped
    command: server /data --console-address ":9001"
    environment:
      MINIO_ROOT_USER: ${MINIO_ROOT_USER:?Set MINIO_ROOT_USER}
      MINIO_ROOT_PASSWORD: ${MINIO_ROOT_PASSWORD:?Set MINIO_ROOT_PASSWORD}
    volumes:
      - object_store_data:/data
    ports:
      - "127.0.0.1:9000:9000"
      - "127.0.0.1:9001:9001"
    networks:
      - data-net
      - app-net
    healthcheck:
      test: ["CMD-SHELL", "curl -f http://localhost:9000/minio/health/live || exit 1"]
      interval: 10s
      timeout: 5s
      retries: 5
```

Start it and open <http://localhost:9001>:

```bash
docker compose --env-file .env -f infra/compose.yaml up -d minio
```

Create three **private** buckets: `landing`, `quarantine`, `backups`. Enable versioning on each. Add lifecycle rules that expire **noncurrent versions after 30 days** (do not configure automatic deletion of current objects).

### S3 policies

Save the following as `infra/minio/policies/pipeline-rw-nodelete.json`:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {"Effect": "Allow", "Action": ["s3:ListBucket", "s3:GetBucketLocation"],
     "Resource": ["arn:aws:s3:::landing", "arn:aws:s3:::quarantine"]},
    {"Effect": "Allow", "Action": ["s3:GetObject", "s3:PutObject"],
     "Resource": ["arn:aws:s3:::landing/*", "arn:aws:s3:::quarantine/*"]}
  ]
}
```

Save as `infra/minio/policies/backup-rw-nodelete.json`:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {"Effect": "Allow", "Action": ["s3:ListBucket", "s3:GetBucketLocation"],
     "Resource": ["arn:aws:s3:::backups"]},
    {"Effect": "Allow", "Action": ["s3:GetObject", "s3:PutObject"],
     "Resource": ["arn:aws:s3:::backups/*"]}
  ]
}
```

In the Silo console, create `pipeline_user` and assign `pipeline-rw-nodelete`; create `backup_user` and assign `backup-rw-nodelete`. Store their credentials in `.env`. Neither policy grants deletion or bucket administration. Note: `PutObject` permits new versions of existing keys, so this is **not** object-lock immutability.

The session used `scripts/test-s3-access.py` with `boto3` to confirm authorized `PutObject` calls succeeded and cross-bucket writes were denied. Full read/delete authorization coverage remains a separate acceptance item.

## 6. Deploy Valkey broker

Add this service:

```yaml
  valkey:
    image: valkey/valkey:8.1.11-alpine
    restart: unless-stopped
    environment:
      VALKEY_PASSWORD: ${VALKEY_PASSWORD:?Set VALKEY_PASSWORD}
    command:
      - /bin/sh
      - -ec
      - |
        exec valkey-server \
          --requirepass "$$VALKEY_PASSWORD" \
          --appendonly yes \
          --appendfsync everysec \
          --maxmemory-policy noeviction
    volumes:
      - valkey_data:/data
    networks:
      - data-net
    healthcheck:
      test: ["CMD-SHELL", "valkey-cli -a \"$$VALKEY_PASSWORD\" --no-auth-warning ping | grep -qx PONG"]
      interval: 10s
      timeout: 5s
      retries: 5
      start_period: 15s
```

Start it:

```bash
docker compose --env-file .env -f infra/compose.yaml up -d valkey
```

Valkey runs only on `data-net`, requires authentication, uses append-only persistence and does not evict queued messages under memory pressure.

## 7. Configure shared Airflow settings

Add this top-level Compose extension **before** `services:`:

```yaml
x-airflow-common: &airflow-common
  image: apache/airflow:3.3.2-python3.12
  restart: unless-stopped
  environment: &airflow-common-env
    AIRFLOW__CORE__EXECUTOR: CeleryExecutor
    AIRFLOW__CORE__FERNET_KEY: ${AIRFLOW_FERNET_KEY:?Set AIRFLOW_FERNET_KEY}
    AIRFLOW__CORE__LOAD_EXAMPLES: "false"
    AIRFLOW__CORE__AUTH_MANAGER: airflow.api_fastapi.auth.managers.simple.simple_auth_manager.SimpleAuthManager
    AIRFLOW__SIMPLE_AUTH_MANAGER__USERS: "admin:admin"
    AIRFLOW__SIMPLE_AUTH_MANAGER__PASSWORDS_FILE: /opt/airflow/simple_auth_manager_passwords.json.generated
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow_user:${AIRFLOW_DB_PASSWORD:?Set AIRFLOW_DB_PASSWORD}@warehouse-db:5432/airflow
    AIRFLOW__CELERY__BROKER_URL: redis://:${VALKEY_PASSWORD:?Set VALKEY_PASSWORD}@valkey:6379/0
    AIRFLOW__CELERY__RESULT_BACKEND: db+postgresql://airflow_user:${AIRFLOW_DB_PASSWORD}@warehouse-db:5432/airflow
    AIRFLOW__CELERY__DEFAULT_QUEUE: default
    AIRFLOW__API_AUTH__JWT_SECRET: ${AIRFLOW_JWT_SECRET:?Set AIRFLOW_JWT_SECRET}
    AIRFLOW__API__BASE_URL: http://airflow-api-server:8080
    AIRFLOW__LOGGING__REMOTE_LOGGING: "false"
  volumes:
    - ../airflow/dags:/opt/airflow/dags
    - ../airflow/plugins:/opt/airflow/plugins
    - airflow_logs:/opt/airflow/logs
    - airflow_config:/opt/airflow/config
  networks:
    - data-net
    - app-net
  depends_on:
    warehouse-db:
      condition: service_healthy
    valkey:
      condition: service_healthy
```

**Password-file caveat:** The path shown above is the location observed in the working deployment. It is **not** covered by the `airflow_config:/opt/airflow/config` volume. The admin password can change on API-server recreation. For a production-quality or clean-rebuild-ready configuration, move the password file to a deliberately persistent location and ensure appropriate ownership/permissions; this improvement was not completed in the session.

Database URL interpolation assumes passwords contain URI-safe characters; the generated hexadecimal values do.

## 8. Initialize Airflow metadata

Add under `services:`:

```yaml
  airflow-init:
    <<: *airflow-common
    restart: "no"
    command:
      - bash
      - -c
      - |
        set -e
        airflow version
        python -c "from airflow.providers.celery.executors.celery_executor import CeleryExecutor; print('CeleryExecutor available')"
        python -c "import psycopg2; print('psycopg2 available')"
        airflow db migrate
        airflow db check
```

Run once:

```bash
docker compose --env-file .env -f infra/compose.yaml up --abort-on-container-exit --exit-code-from airflow-init airflow-init
```

The init service should exit with code 0; it is not intended to remain running.

## 9. Start Airflow control-plane services

Add:

```yaml
  airflow-api-server:
    <<: *airflow-common
    command: api-server
    ports:
      - "127.0.0.1:8080:8080"
    healthcheck:
      test: ["CMD", "curl", "--fail", "http://localhost:8080/api/v2/monitor/health"]
      interval: 30s
      timeout: 10s
      retries: 5
      start_period: 60s
    depends_on:
      warehouse-db:
        condition: service_healthy
      valkey:
        condition: service_healthy
      airflow-init:
        condition: service_completed_successfully

  airflow-scheduler:
    <<: *airflow-common
    command: scheduler
    depends_on:
      warehouse-db:
        condition: service_healthy
      valkey:
        condition: service_healthy
      airflow-init:
        condition: service_completed_successfully

  airflow-dag-processor:
    <<: *airflow-common
    command: dag-processor
    depends_on:
      warehouse-db:
        condition: service_healthy
      valkey:
        condition: service_healthy
      airflow-init:
        condition: service_completed_successfully
```

Start them:

```bash
docker compose --env-file .env -f infra/compose.yaml up -d airflow-api-server airflow-scheduler airflow-dag-processor
```

The UI is at <http://localhost:8080>. The generated Simple Auth Manager password can be read **privately** inside the API-server container at `/opt/airflow/simple_auth_manager_passwords.json.generated`. Never commit or paste its contents into a shared issue.

The API health endpoint `/api/v2/monitor/health` was verified with healthy metadata, scheduler and DAG processor. The triggerer was not deployed.

## 10. Start Celery workers

Add three services:

```yaml
  airflow-worker-general-1:
    <<: *airflow-common
    command: celery worker --queues default,ingest
    environment:
      <<: *airflow-common-env
      AIRFLOW__CELERY__WORKER_CONCURRENCY: "4"
    depends_on:
      warehouse-db:
        condition: service_healthy
      valkey:
        condition: service_healthy
      airflow-init:
        condition: service_completed_successfully

  airflow-worker-general-2:
    <<: *airflow-common
    command: celery worker --queues default,ingest
    environment:
      <<: *airflow-common-env
      AIRFLOW__CELERY__WORKER_CONCURRENCY: "4"
    depends_on:
      warehouse-db:
        condition: service_healthy
      valkey:
        condition: service_healthy
      airflow-init:
        condition: service_completed_successfully

  airflow-worker-batch:
    <<: *airflow-common
    command: celery worker --queues batch
    environment:
      <<: *airflow-common-env
      AIRFLOW__CELERY__WORKER_CONCURRENCY: "2"
    depends_on:
      warehouse-db:
        condition: service_healthy
      valkey:
        condition: service_healthy
      airflow-init:
        condition: service_completed_successfully
```

`AIRFLOW__CELERY__DEFAULT_QUEUE=default` determines where tasks without a queue assignment are sent. **Worker queue subscriptions are defined by `--queues`**, not by `DEFAULT_QUEUE`. Concurrency is the maximum number of simultaneous tasks per worker; it is not a CPU reservation.

Start the workers:

```bash
docker compose --env-file .env -f infra/compose.yaml up -d airflow-worker-general-1 airflow-worker-general-2 airflow-worker-batch
```

## 11. Add and run the smoke DAG

Create `airflow/dags/infrastructure_smoke_test.py`:

```python
import logging
import socket
from datetime import datetime

from airflow.sdk import dag, task

logger = logging.getLogger(__name__)


@dag(
    dag_id="infrastructure_smoke_test",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["infrastructure", "smoke-test"],
)
def infrastructure_smoke_test():
    @task(queue="default")
    def test_default_queue():
        hostname = socket.gethostname()
        logger.info("DEFAULT queue task executed on %s", hostname)
        return hostname

    @task(queue="ingest")
    def test_ingest_queue():
        hostname = socket.gethostname()
        logger.info("INGEST queue task executed on %s", hostname)
        return hostname

    @task(queue="batch")
    def test_batch_queue():
        hostname = socket.gethostname()
        logger.info("BATCH queue task executed on %s", hostname)
        return hostname

    default = test_default_queue()
    ingest = test_ingest_queue()
    batch = test_batch_queue()
    [default, ingest] >> batch


infrastructure_smoke_test()
```

Open the Airflow UI, manually trigger `infrastructure_smoke_test`, and confirm that all three tasks succeed. The first two should run on general workers; the last should run on the batch worker. This smoke test **passed** in the working session.

## 12. Routine operations

```bash
# Validate Compose
docker compose --env-file .env -f infra/compose.yaml config --quiet

# Start the stack
docker compose --env-file .env -f infra/compose.yaml up -d

# Show status
docker compose --env-file .env -f infra/compose.yaml ps

# View logs for one service
docker compose --env-file .env -f infra/compose.yaml logs --tail=100 airflow-scheduler

# Stop without deleting data
docker compose --env-file .env -f infra/compose.yaml down
```

**Do not use `down -v` on a populated environment** unless you intentionally want to remove PostgreSQL, Silo, Valkey and Airflow persistent data.

## 13. Scope and remaining Phase 1 work

Completed in the working session: repository foundations, two PostgreSQL services, private versioned Silo buckets and service policies, Valkey broker, Airflow metadata migration, API server, scheduler, DAG processor, three Celery workers, and a successful three-queue smoke DAG.

Still to finish before declaring the **original** Phase 1 plan fully complete:

- Consolidate database initialization into clean first-boot scripts and test a fresh deployment.
- Automate Silo bucket/policy/user creation rather than relying on manual console setup.
- Persist the Airflow admin password deliberately and restrict its visibility to appropriate components.
- Add resource limits and any remaining service healthchecks/operational shortcuts specified by the original plan.
- Complete role-isolation, S3 read/delete-denial and security acceptance checks.
- Decide whether to add the optional Flower UI and the more extensive multi-worker acceptance smoke test from the original plan.

This guide is intentionally a chronological implementation reference, without a record of failed commands or troubleshooting.
