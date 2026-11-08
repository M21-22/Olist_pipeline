# Phase 1 Guide (CeleryExecutor edition): Infrastructure Foundation

**Goal:** one command brings up a healthy, locked-down base stack: source Postgres, warehouse Postgres, S3-compatible object storage, a message broker, and Airflow with **multiple Celery worker instances**. No application logic yet, except a smoke DAG that proves work is spread across workers.
**Effort:** 4-5 days (the Celery additions add about a day). **Result:** `make reset && make up` gives all services healthy, least-privilege roles and bucket policies in place, and `make smoke` shows tasks running on different worker containers.

This replaces `phase1-guide.md` (LocalExecutor). Section 0 lists every difference.

---

## 0. What changed compared with the LocalExecutor guide

### 0.1 Architecture and services

| Item | Before (LocalExecutor) | After (CeleryExecutor) |
|---|---|---|
| Executor | LocalExecutor (tasks run inside the scheduler container) | **CeleryExecutor** (tasks run on separate worker containers) |
| Broker | none | **Valkey (Redis-compatible), password-protected, AOF persistence** |
| Worker containers | 0 (tasks inside the scheduler) | **2 replicas of `airflow-worker` (queues `default`, `ingest`) + 1 `airflow-worker-batch` (queue `batch`)** |
| Task monitoring UI | Airflow UI only | Airflow UI **+ optional Flower (profile `monitoring`)** |
| Celery result backend | n/a | **Metadata DB (`db+postgresql://`)** |
| Admin-password file mounted into | all Airflow services | **API server only** (workers no longer see it) |
| Smoke test | none | **`phase1_smoke_celery` DAG + `make smoke`** |
| Long-running services in `compose.yaml` | 6 | **9** (10 with Flower) |
| Long-running containers (each worker replica counts) | 6 | **10** (11 with Flower) |

### 0.2 Memory limits (caps, not typical use), tracked to the MiB

| Container(s) | Before | After |
|---|---|---|
| source-db | 384 MiB | 384 MiB |
| warehouse-db | 1024 MiB | 1024 MiB |
| minio | 512 MiB | 512 MiB |
| airflow-apiserver | 1024 MiB | 1024 MiB |
| airflow-scheduler | 1024 MiB | 1024 MiB |
| airflow-dag-processor | 1024 MiB | 1024 MiB |
| **valkey** | **none** | **192 MiB** |
| **airflow-worker (2 replicas × 1536 MiB)** | **none** | **3072 MiB** |
| **airflow-worker-batch (1 × 2048 MiB)** | **none** | **2048 MiB** |
| **Total, without Flower** | **4992 MiB (4.8750 GiB)** | **10304 MiB (10.0625 GiB)** |
| **airflow-flower (optional profile)** | **none** | **256 MiB** |
| **Total, with Flower** | **4992 MiB (4.8750 GiB)** | **10560 MiB (10.3125 GiB)** |
| **Increase caused by Celery** | | **+5312 MiB (+5.1875 GiB) without Flower; +5568 MiB (+5.4375 GiB) with Flower** |

These are worst-case caps. Actual use is normally well below them; measure with `make stats`.

**Projected total after all later phases** (Grafana 256, Prometheus 256, statsd-exporter 64, API 256, emulator 256, dbt runner 1024, mail sink 64 = 2176 MiB more): 10560 + 2176 = **12736 MiB (12.4375 GiB)** worst case. With your 16 GB available, **give Docker 16 GB** (leaves 3.5625 GiB of slack at the worst case); 12 GB is enough for Phases 1-4 only.

### 0.3 Other configuration changes

| Setting | Before | After |
|---|---|---|
| Docker memory recommendation | at least 8 GB | **16 GB recommended (12 GB minimum for Phases 1-4)**; also give Docker at least 6 CPUs |
| `airflow_meta` connection limit | 30 | **50** |
| Warehouse `max_connections` | default (100) | **150** |
| Airflow `parallelism` | default | **12** (2×4 + 1×2 = 10 worker slots, plus headroom) |
| Broker visibility timeout | n/a | **21600 s (6 h)** |
| `requirements.txt` | postgres, amazon providers | **+ celery provider, redis client** |
| New `.env` keys | | **`VALKEY_IMAGE`, `VALKEY_PASSWORD`, `FLOWER_USER`, `FLOWER_PASSWORD`** |

---

## 1. Read this first

### 1.1 Upstream MinIO is no longer usable as I first described it

I recommended MinIO earlier without checking its status. I have now checked:

- Pre-built binaries and Docker images from MinIO stopped around October 2025, and the community edition became source-only.
- The project went into "maintenance mode" on 3 December 2025, and the repository was later archived (February 2026 per one maintainer write-up; others say April).
- The admin console (users, policies, lifecycle) was stripped from the community edition in May 2025.

| Option | Pros | Cons |
|---|---|---|
| **Community fork of MinIO** (the `pgsty` project; repo renamed from `pgsty/minio` to `pgsty/silo` in August 2026) | Drop-in compatible: same `mc` commands, policies, service accounts. CVE-patched builds with the admin console restored. | Maintained largely by one person; AGPL; image name and tags need verifying |
| **RustFS** (Apache-2.0) | Described as the closest general replacement | I have not verified bucket versioning and scoped IAM policies, which this design needs |
| **SeaweedFS** | Mature, feature-rich | More moving parts |
| **Garage** | Light and simple | One comparison says it lacks bucket policies and versioning, which breaks this design |
| **Build MinIO from source** | Official code | You own patching, scanning, signing |

**Decision:** use the MinIO-compatible fork, behind environment variables (`S3_IMAGE`, endpoint, keys) so swapping is a configuration change. Write it up as an ADR. Check Docker Hub for the exact image name and a recent tag; I could not verify them. The service name stays `minio` and the variables stay `MINIO_*`.

### 1.2 Versions I verified

| Component | Pin | Why |
|---|---|---|
| Apache Airflow | **3.3.2** (released 17 Sep 2026) | Current stable on the 3.x line |
| PostgreSQL | **17** | Airflow 3.3 lists PostgreSQL 13-17 as supported (a third-party summary, so re-check). PostgreSQL 18 is the newest stable major; PostgreSQL 19 is a release candidate. Postgres 18 images also moved the data directory, so 17 keeps the volume path below valid. |
| Broker | **Valkey 8** (BSD-3-Clause, Linux Foundation) | Drop-in Redis-compatible fork. Redis returned to open source under AGPLv3 with Redis 8, so Redis is also acceptable; the config is identical. Check the exact image tag. |

### 1.3 Why CeleryExecutor here, and what it costs

**What it buys you in a portfolio:**
- Several worker instances pulling tasks from a queue (horizontal scaling, visible in Airflow's task details and Flower).
- **Queue isolation:** ingest tasks and heavy batch tasks (reconciliation, backfills) run on different worker pools, so a long backfill cannot starve the 15-minute ingest.
- A realistic failure story: kill a worker, and Airflow reschedules its tasks elsewhere.

**What it costs (be honest about this in the README):**
- A broker is one more component and one more single point of failure. Persistence (AOF) and `noeviction` are set so a restart does not silently lose queued tasks, but the design is not highly available.
- About 5.2 GiB more memory cap (Section 0.2).
- All workers run on **one host**. This demonstrates a distributed *design*, not hardware fault tolerance. To run workers on other machines you would also need DAGs delivered to every host (git-sync or an image with DAGs baked in) and remote task logging to the object store, since the shared volume used here only works on one host.

**Broker choice (ADR material):**

| Broker | Verdict |
|---|---|
| **Valkey** | Chosen: tiny footprint, BSD licence, simple. Caveat: the Redis transport re-delivers a task that runs longer than the *visibility timeout*, so we set it to 6 hours (the default is 1 hour), well above any task in this project. |
| Redis 8 | Equivalent to Valkey for this use; AGPLv3. |
| RabbitMQ | Stronger delivery semantics and no visibility-timeout issue; heavier (several hundred MB). Revisit if tasks become very long. |

---

## 2. Prerequisites

- Docker Engine or Docker Desktop with **Compose v2** (`docker compose version`)
- **Docker allocation: 16 GB RAM recommended (12 GB minimum for Phases 1-4), at least 6 CPUs.** On Docker Desktop: Settings → Resources.
- `make`, `openssl`, `git`, `bash`
- Windows: use WSL2 and keep the repo inside the WSL filesystem. See the line-endings note in Step 1.

---

## 3. What you will have at the end

```
 host (127.0.0.1 only)
   :8080 Airflow UI   :9001 object-store console   :9000 S3 API   [:5555 Flower]
 ┌────────────────────────── app-net ───────────────────────────────┐
 │  airflow-apiserver     minio (console only)     [airflow-flower]  │
 └──────────────────────────┼──────────────────────────┼─────────────┘
 ┌───────────────── data-net  (internal: no outside access) ──────────┐
 │ source-db     warehouse-db (+ airflow metadata DB)     minio        │
 │ valkey (broker, password, AOF)                                       │
 │ airflow-apiserver   airflow-scheduler   airflow-dag-processor        │
 │ airflow-worker ×2        queues: default, ingest   (4 slots each)    │
 │ airflow-worker-batch ×1  queue:  batch             (2 slots)         │
 └──────────────────────────────────────────────────────────────────────┘
 one-shot jobs: minio-init (buckets, policies, users), airflow-init (migrate, auth file)
```

- `data-net` is `internal: true`: containers on it cannot reach the internet. Anything needing the outside world is done at image build time.
- Only the Airflow UI, the object-store ports, and (optionally) Flower are published, and only on `127.0.0.1`. Database and broker ports are **not** published; use `make psql-wh` and `make psql-src`.
- Task flow: scheduler → Valkey queue (`default`, `ingest`, or `batch`) → a worker that listens on that queue → task runs, talks to the API server (task execution API) and the databases/object store.

---

## 4. Step-by-step

### Step 1: Folders and git hygiene

```bash
mkdir -p infra/postgres/{source-init,warehouse-init} infra/minio/policies \
         airflow/{dags,plugins,include} scripts data
touch airflow/plugins/.gitkeep airflow/include/.gitkeep
```

`.gitignore` (add these):

```gitignore
.env
data/
airflow/logs/
**/__pycache__/
```

`.gitattributes` (important on Windows; CRLF breaks `.sh` files in containers):

```gitattributes
*.sh   text eol=lf
*.sql  text eol=lf
*.yaml text eol=lf
Makefile text eol=lf
```

### Step 2: `.env.example` and secrets

Commit `.env.example`; never commit `.env`. Leave every secret empty so the generator fills it.

```dotenv
# ---- Images (pin tags; verify they exist) ----
POSTGRES_IMAGE=postgres:17
VALKEY_IMAGE=valkey/valkey:8      # verify tag; redis:8 works the same way
AIRFLOW_VERSION=3.3.2
AIRFLOW_PYTHON=3.12
S3_IMAGE=            # MinIO-compatible server image, e.g. the pgsty fork: verify name + tag
MC_IMAGE=            # image containing the `mc` client: verify, or use the build fallback in Step 5
AIRFLOW_UID=50000    # on Linux set to `id -u`

# ---- PostgreSQL (source) ----
POSTGRES_SOURCE_PASSWORD=
POSTGRES_SOURCE_EMULATOR_PASSWORD=
POSTGRES_SOURCE_EXTRACTOR_PASSWORD=

# ---- PostgreSQL (warehouse) ----
POSTGRES_WAREHOUSE_PASSWORD=
POSTGRES_ETL_LOADER_PASSWORD=
POSTGRES_DBT_RUNNER_PASSWORD=
POSTGRES_SODA_READER_PASSWORD=
POSTGRES_API_READER_PASSWORD=
POSTGRES_GRAFANA_READER_PASSWORD=
POSTGRES_AIRFLOW_META_PASSWORD=

# ---- Broker ----
VALKEY_PASSWORD=

# ---- Airflow ----
AIRFLOW_FERNET_KEY=
AIRFLOW_SECRET_KEY=
AIRFLOW_JWT_SECRET=
AIRFLOW_ADMIN_USER=admin
AIRFLOW_ADMIN_PASSWORD=
FLOWER_USER=flower
FLOWER_PASSWORD=

# ---- Object store (MinIO-compatible) ----
MINIO_ACCESS_KEY=minio_root
MINIO_SECRET_KEY=
MINIO_AIRFLOW_ACCESS_KEY=airflow_svc
MINIO_AIRFLOW_SECRET_KEY=
MINIO_BACKUP_ACCESS_KEY=backup_svc
MINIO_BACKUP_SECRET_KEY=

# ---- Later phases (leave empty for now) ----
API_KEY_SALT=
GRAFANA_ADMIN_PASSWORD=
GRAFANA_DB_PASSWORD=
```

Update `scripts/gen-secrets.sh` from the earlier message with these additions:

```bash
# add to the `for v in ...` loop:
  POSTGRES_SOURCE_EMULATOR_PASSWORD POSTGRES_SOURCE_EXTRACTOR_PASSWORD \
  VALKEY_PASSWORD FLOWER_PASSWORD \

# add after the loop:
set_if_unset AIRFLOW_JWT_SECRET "$(hex 32)"
```

All generated values are hex (or URL-safe base64 for the Fernet key), which matters now more than before: the broker password is embedded in a URL (`redis://:PASSWORD@valkey:6379/0`), and characters like `/`, `+`, `@` would break it.

```bash
bash scripts/gen-secrets.sh
# then edit .env: fill S3_IMAGE and MC_IMAGE (see Step 5), and AIRFLOW_UID on Linux
```

### Step 3: Source database init

PostgreSQL's official image runs everything in `/docker-entrypoint-initdb.d/` **once**, on first start with an empty data directory. It cannot read environment variables inside `.sql` files, so use a `.sh` script and pass passwords to `psql` as variables.

`infra/postgres/source-init/01-init.sh`:

```bash
#!/bin/bash
set -euo pipefail

psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d source \
  -v emulator_pw="$EMULATOR_PW" -v extractor_pw="$EXTRACTOR_PW" <<'SQL'

-- Two application roles, no superuser, no role creation
CREATE ROLE emulator  LOGIN PASSWORD :'emulator_pw'  CONNECTION LIMIT 10;
CREATE ROLE extractor LOGIN PASSWORD :'extractor_pw' CONNECTION LIMIT 10;

-- Nobody connects by default
REVOKE ALL ON DATABASE source FROM PUBLIC;
GRANT CONNECT ON DATABASE source TO emulator, extractor;
REVOKE CREATE ON SCHEMA public FROM PUBLIC;

-- The "application" schema, owned by the emulator (it plays the shop's app)
CREATE SCHEMA shop AUTHORIZATION emulator;

-- Ground truth for the defect harness: the extractor must NEVER see this
CREATE SCHEMA chaos AUTHORIZATION emulator;

-- The extractor is read-only on shop, including tables created later by the emulator
GRANT USAGE ON SCHEMA shop TO extractor;
ALTER DEFAULT PRIVILEGES FOR ROLE emulator IN SCHEMA shop GRANT SELECT ON TABLES TO extractor;
ALTER ROLE extractor SET default_transaction_read_only = on;
ALTER ROLE extractor SET statement_timeout = '60s';
ALTER ROLE extractor SET idle_in_transaction_session_timeout = '60s';
SQL
```

Changed from the LocalExecutor guide: the extractor's connection limit goes from 5 to 10, because several workers can now extract tables in parallel (one mapped task per table).

Phase 2 note: create the `shop` tables **as the `emulator` role** so the default privileges apply.

### Step 4: Warehouse database init (roles, schemas, grants)

One server hosts two databases: `warehouse` (analytics) and `airflow` (metadata and Celery results), with separate owners.

`infra/postgres/warehouse-init/01-init.sh`:

```bash
#!/bin/bash
set -euo pipefail

# Part 1: roles and the Airflow metadata database (connect to the maintenance DB)
psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d postgres \
  -v etl_pw="$ETL_LOADER_PW"  -v dbt_pw="$DBT_RUNNER_PW" \
  -v soda_pw="$SODA_READER_PW" -v api_pw="$API_READER_PW" \
  -v graf_pw="$GRAFANA_READER_PW" -v meta_pw="$AIRFLOW_META_PW" <<'SQL'

CREATE ROLE etl_loader     LOGIN PASSWORD :'etl_pw'  CONNECTION LIMIT 30;
CREATE ROLE dbt_runner     LOGIN PASSWORD :'dbt_pw'  CONNECTION LIMIT 20;
CREATE ROLE soda_reader    LOGIN PASSWORD :'soda_pw' CONNECTION LIMIT 5;
CREATE ROLE api_reader     LOGIN PASSWORD :'api_pw'  CONNECTION LIMIT 10;
CREATE ROLE grafana_reader LOGIN PASSWORD :'graf_pw' CONNECTION LIMIT 5;
CREATE ROLE airflow_meta   LOGIN PASSWORD :'meta_pw' CONNECTION LIMIT 50;

CREATE DATABASE airflow OWNER airflow_meta;
REVOKE ALL ON DATABASE airflow FROM PUBLIC;
SQL

# Part 2: the warehouse database
psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d warehouse <<'SQL'

-- Lock the door, then open it only for named roles.
-- (airflow_meta is deliberately NOT granted CONNECT on warehouse.)
REVOKE ALL ON DATABASE warehouse FROM PUBLIC;
GRANT CONNECT ON DATABASE warehouse
  TO etl_loader, dbt_runner, soda_reader, api_reader, grafana_reader;
REVOKE CREATE ON SCHEMA public FROM PUBLIC;

CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

-- Schemas. Whoever owns a schema creates its tables.
CREATE SCHEMA raw;                                   -- owner: warehouse_admin (migrations)
CREATE SCHEMA audit;                                 -- owner: warehouse_admin (migrations)
CREATE SCHEMA staging    AUTHORIZATION dbt_runner;
CREATE SCHEMA snapshots  AUTHORIZATION dbt_runner;
CREATE SCHEMA marts      AUTHORIZATION dbt_runner;
CREATE SCHEMA api        AUTHORIZATION dbt_runner;
CREATE SCHEMA quarantine AUTHORIZATION dbt_runner;   -- row-level rejects, written by dbt

-- ---------- raw: loader writes, others read ----------
GRANT USAGE ON SCHEMA raw TO etl_loader, dbt_runner, soda_reader;
ALTER DEFAULT PRIVILEGES FOR ROLE warehouse_admin IN SCHEMA raw
  GRANT SELECT, INSERT, DELETE ON TABLES TO etl_loader;   -- DELETE: idempotent batch reload
ALTER DEFAULT PRIVILEGES FOR ROLE warehouse_admin IN SCHEMA raw
  GRANT SELECT ON TABLES TO dbt_runner, soda_reader;
ALTER DEFAULT PRIVILEGES FOR ROLE warehouse_admin IN SCHEMA raw
  GRANT USAGE, SELECT ON SEQUENCES TO etl_loader;

-- ---------- audit: loader writes, everyone downstream reads ----------
GRANT USAGE ON SCHEMA audit TO etl_loader, dbt_runner, soda_reader, grafana_reader;
ALTER DEFAULT PRIVILEGES FOR ROLE warehouse_admin IN SCHEMA audit
  GRANT SELECT, INSERT, UPDATE ON TABLES TO etl_loader;
ALTER DEFAULT PRIVILEGES FOR ROLE warehouse_admin IN SCHEMA audit
  GRANT SELECT ON TABLES TO dbt_runner, soda_reader, grafana_reader;
ALTER DEFAULT PRIVILEGES FOR ROLE warehouse_admin IN SCHEMA audit
  GRANT USAGE, SELECT ON SEQUENCES TO etl_loader;

-- ---------- dbt-owned schemas: who may read what dbt builds ----------
GRANT USAGE ON SCHEMA staging TO etl_loader, soda_reader;       -- etl_loader: reconciliation
ALTER DEFAULT PRIVILEGES FOR ROLE dbt_runner IN SCHEMA staging
  GRANT SELECT ON TABLES TO etl_loader, soda_reader;

GRANT USAGE ON SCHEMA marts TO soda_reader;
ALTER DEFAULT PRIVILEGES FOR ROLE dbt_runner IN SCHEMA marts
  GRANT SELECT ON TABLES TO soda_reader;

GRANT USAGE ON SCHEMA quarantine TO soda_reader, grafana_reader;
ALTER DEFAULT PRIVILEGES FOR ROLE dbt_runner IN SCHEMA quarantine
  GRANT SELECT ON TABLES TO soda_reader, grafana_reader;

-- api: the ONLY schema the API role can touch
GRANT USAGE ON SCHEMA api TO api_reader, grafana_reader, soda_reader;
ALTER DEFAULT PRIVILEGES FOR ROLE dbt_runner IN SCHEMA api
  GRANT SELECT ON TABLES TO api_reader, grafana_reader, soda_reader;

-- ---------- guard rails per role ----------
ALTER ROLE api_reader     SET default_transaction_read_only = on;
ALTER ROLE api_reader     SET statement_timeout = '10s';
ALTER ROLE api_reader     SET idle_in_transaction_session_timeout = '15s';
ALTER ROLE grafana_reader SET default_transaction_read_only = on;
ALTER ROLE grafana_reader SET statement_timeout = '30s';
ALTER ROLE soda_reader    SET default_transaction_read_only = on;
ALTER ROLE soda_reader    SET statement_timeout = '120s';
SQL
```

**Changed from the LocalExecutor guide:** `etl_loader` connection limit 20 → **30** (parallel mapped load tasks on several workers), `airflow_meta` 30 → **50** (Celery workers' result-backend connections), plus `max_connections=150` on the server in Step 7.

**If you already started the stack with the old limits** (init scripts do not rerun): `make reset`, or change them live:

```sql
ALTER ROLE airflow_meta CONNECTION LIMIT 50;
ALTER ROLE etl_loader   CONNECTION LIMIT 30;
ALTER ROLE extractor    CONNECTION LIMIT 10;   -- run this one on source-db
```

**Permission matrix** (`S` select, `I` insert, `U` update, `D` delete, `own` owns/creates):

| Role | raw | audit | staging | marts | quarantine | api |
|---|---|---|---|---|---|---|
| `warehouse_admin` | own | own | | | | |
| `etl_loader` | S I D | S I U | S | | | |
| `dbt_runner` | S | S | own | own | own | own |
| `soda_reader` | S | S | S | S | S | S |
| `grafana_reader` | | S | | | S | S |
| `api_reader` | | | | | | S |
| `airflow_meta` | no access to `warehouse`; owns the `airflow` database | | | | | |

Two deviations from the original plan, on purpose:
- Batch-level quarantine records live in `audit.batch_registry` and the MinIO `quarantine` bucket; the `quarantine` schema is only for row-level rejects that dbt writes. Each schema keeps a single writer.
- Default privileges only apply to tables created later **by the role named in `FOR ROLE`**, so migrations in `raw` and `audit` must run as `warehouse_admin`, and dbt must connect as `dbt_runner`.

### Step 5: Object store bootstrap

`infra/minio/policies/airflow-rw.json` (Airflow can read and write landing and quarantine, **not delete**; landing is treated as immutable, and quarantining means *copying* the batch, not moving it):

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:ListBucket", "s3:GetBucketLocation"],
      "Resource": ["arn:aws:s3:::landing", "arn:aws:s3:::quarantine"]
    },
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject"],
      "Resource": ["arn:aws:s3:::landing/*", "arn:aws:s3:::quarantine/*"]
    }
  ]
}
```

This policy now applies to every worker container, since all workers use the same service account. That is acceptable here; Phase 11 can split credentials per queue (for example, only the `batch` workers get the backup role).

`infra/minio/policies/backup-rw.json` (backups: read, write, delete for retention; landing: read only):

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:ListBucket", "s3:GetBucketLocation"],
      "Resource": ["arn:aws:s3:::backups", "arn:aws:s3:::landing"]
    },
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
      "Resource": ["arn:aws:s3:::backups/*"]
    },
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject"],
      "Resource": ["arn:aws:s3:::landing/*"]
    }
  ]
}
```

`infra/minio/init.sh` (idempotent; safe to rerun):

```sh
#!/bin/sh
set -eu

mc alias set local http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null

# Buckets, with versioning (protects against overwrite/deletion mistakes)
for b in landing quarantine backups; do
  mc mb --ignore-existing "local/$b"
  mc version enable "local/$b"
done

# Versioning keeps every overwrite; expire old versions so disk use stays bounded.
# (Flag name may differ by mc version: check `mc ilm rule add --help`.)
mc ilm rule add --noncurrent-expire-days 30 local/landing    || echo "WARN: ILM rule not applied"
mc ilm rule add --noncurrent-expire-days 30 local/quarantine || echo "WARN: ILM rule not applied"

# Policies (re-creating overwrites) and service users
mc admin policy create local airflow-rw /policies/airflow-rw.json
mc admin policy create local backup-rw  /policies/backup-rw.json

mc admin user add local "$MINIO_AIRFLOW_ACCESS_KEY" "$MINIO_AIRFLOW_SECRET_KEY"
mc admin policy attach local airflow-rw --user "$MINIO_AIRFLOW_ACCESS_KEY"

mc admin user add local "$MINIO_BACKUP_ACCESS_KEY" "$MINIO_BACKUP_SECRET_KEY"
mc admin policy attach local backup-rw --user "$MINIO_BACKUP_ACCESS_KEY"

echo "object store bootstrap complete"
```

Command names for policy handling changed between `mc` versions (`policy add` vs `policy create`, `set` vs `attach`). Use `mc admin policy --help` in your pinned image and adjust.

**Fallback if no ready-made image includes `mc`:** build it from source. `infra/minio/Dockerfile.mc`:

```dockerfile
FROM golang:1.23 AS build
RUN go install github.com/minio/mc@latest

FROM alpine:3.20
COPY --from=build /go/bin/mc /usr/local/bin/mc
ENTRYPOINT ["/bin/sh"]
```

Then give `minio-init` a `build:` section pointing at it instead of `image: ${MC_IMAGE}`. The upstream client is unmaintained, so pin the version and record the risk in your ADR. Use a Go image at least as new as the client requires.

### Step 6: Airflow image and init

`airflow/requirements.txt`:

```text
apache-airflow-providers-postgres
apache-airflow-providers-amazon
apache-airflow-providers-celery
redis
```

(The official image already bundles Celery support; listing it makes the dependency explicit and keeps it pinned by the constraints file. Later phases add `pyarrow`, `great-expectations`, and so on, **after** checking them against Airflow's constraints.)

`airflow/Dockerfile`:

```dockerfile
ARG AIRFLOW_VERSION=3.3.2
ARG AIRFLOW_PYTHON=3.12
FROM apache/airflow:${AIRFLOW_VERSION}-python${AIRFLOW_PYTHON}
ARG AIRFLOW_VERSION
ARG AIRFLOW_PYTHON
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${AIRFLOW_PYTHON}.txt"
```

With Celery the **same image** runs the API server, scheduler, DAG processor, and every worker, so all of them have identical dependencies. dbt stays **out** of this image (own container in Phase 5) because its dependencies conflict with Airflow's.

`airflow/init.sh` (runs once as root: creates the auth file, fixes permissions, migrates the metadata DB):

```bash
#!/bin/bash
set -euo pipefail
umask 077
mkdir -p /opt/airflow/logs /opt/airflow/auth

# Admin login for Airflow's simple auth manager, generated from .env (never stored in git)
printf '{"%s": "%s"}\n' "${AIRFLOW_ADMIN_USER}" "${AIRFLOW_ADMIN_PASSWORD}" \
  > /opt/airflow/auth/passwords.json

chown -R "${AIRFLOW_UID}:0" /opt/airflow/logs /opt/airflow/auth

# The image's own entrypoint runs `airflow db migrate` when _AIRFLOW_DB_MIGRATE=true
exec /entrypoint airflow version
```

### Step 7: `infra/compose.yaml`

Notes before the file:

- Paths are relative to the **compose file's directory** (`infra/`).
- Connections to Postgres and the object store are provided as `AIRFLOW_CONN_*` environment variables, so their credentials are **not** stored in Airflow's metadata database. Workers get them through the shared environment block.
- The broker, workers, and (optionally) Flower are new; everything else is as before except where marked `# CHANGED` or `# NEW`.
- Resource limits are starting points. Measure with `docker stats` and adjust.

```yaml
name: olist

x-logging: &default-logging
  driver: json-file
  options: { max-size: "10m", max-file: "3" }     # keeps logs from filling the disk

x-airflow-env: &airflow-env
  AIRFLOW__CORE__EXECUTOR: CeleryExecutor                                  # CHANGED (was LocalExecutor)
  AIRFLOW__CORE__PARALLELISM: "12"                                         # NEW
  AIRFLOW__CORE__LOAD_EXAMPLES: "false"
  AIRFLOW__CORE__DEFAULT_TIMEZONE: utc
  AIRFLOW__CORE__FERNET_KEY: ${AIRFLOW_FERNET_KEY:?run make secrets}
  AIRFLOW__CORE__EXECUTION_API_SERVER_URL: http://airflow-apiserver:8080/execution/
  AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow_meta:${POSTGRES_AIRFLOW_META_PASSWORD:?}@warehouse-db:5432/airflow
  AIRFLOW__CELERY__BROKER_URL: redis://:${VALKEY_PASSWORD:?}@valkey:6379/0                         # NEW
  AIRFLOW__CELERY__RESULT_BACKEND: db+postgresql://airflow_meta:${POSTGRES_AIRFLOW_META_PASSWORD:?}@warehouse-db:5432/airflow   # NEW
  AIRFLOW__CELERY_BROKER_TRANSPORT_OPTIONS__VISIBILITY_TIMEOUT: "21600"    # NEW: 6 h, so long tasks are not re-delivered
  AIRFLOW__API__SECRET_KEY: ${AIRFLOW_SECRET_KEY:?}
  AIRFLOW__API_AUTH__JWT_SECRET: ${AIRFLOW_JWT_SECRET:?}
  AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS: "${AIRFLOW_ADMIN_USER:?}:admin"
  AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE: /opt/airflow/auth/passwords.json
  PYTHONPATH: /opt/airflow/include
  # Connections as env vars (not stored in the metadata DB)
  AIRFLOW_CONN_SOURCE_DB: postgres://extractor:${POSTGRES_SOURCE_EXTRACTOR_PASSWORD:?}@source-db:5432/source
  AIRFLOW_CONN_WAREHOUSE_ETL: postgres://etl_loader:${POSTGRES_ETL_LOADER_PASSWORD:?}@warehouse-db:5432/warehouse
  AIRFLOW_CONN_S3_LANDING: aws://${MINIO_AIRFLOW_ACCESS_KEY:?}:${MINIO_AIRFLOW_SECRET_KEY:?}@/?endpoint_url=http%3A%2F%2Fminio%3A9000&region_name=us-east-1

x-airflow-common: &airflow-common
  image: olist-airflow:${AIRFLOW_VERSION:-3.3.2}
  user: "${AIRFLOW_UID:-50000}:0"
  environment: *airflow-env
  volumes:                                         # CHANGED: the auth file is no longer mounted here
    - ../airflow/dags:/opt/airflow/dags:ro
    - ../airflow/plugins:/opt/airflow/plugins:ro
    - ../airflow/include:/opt/airflow/include:ro
    - airflow-logs:/opt/airflow/logs               # shared so the API server can show worker logs (single host)
  networks: [data-net]
  logging: *default-logging
  security_opt: ["no-new-privileges:true"]
  depends_on:
    airflow-init: { condition: service_completed_successfully }
    valkey:       { condition: service_healthy }   # NEW

services:
  # ------------------------------------------------------------ source DB
  source-db:
    image: ${POSTGRES_IMAGE:-postgres:17}
    restart: unless-stopped
    environment:
      POSTGRES_DB: source
      POSTGRES_USER: source_admin
      POSTGRES_PASSWORD: ${POSTGRES_SOURCE_PASSWORD:?}
      EMULATOR_PW: ${POSTGRES_SOURCE_EMULATOR_PASSWORD:?}
      EXTRACTOR_PW: ${POSTGRES_SOURCE_EXTRACTOR_PASSWORD:?}
    command: >
      postgres
      -c log_connections=on -c log_disconnections=on
      -c log_min_duration_statement=1000
    volumes:
      - source-db-data:/var/lib/postgresql/data
      - ./postgres/source-init:/docker-entrypoint-initdb.d:ro
    networks: [data-net]
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U source_admin -d source"]
      interval: 10s
      timeout: 5s
      retries: 10
      start_period: 20s
    deploy: { resources: { limits: { cpus: "1.0", memory: 384M } } }
    logging: *default-logging
    security_opt: ["no-new-privileges:true"]

  # ------------------------------------------------------------ warehouse DB (+ airflow metadata DB)
  warehouse-db:
    image: ${POSTGRES_IMAGE:-postgres:17}
    restart: unless-stopped
    environment:
      POSTGRES_DB: warehouse
      POSTGRES_USER: warehouse_admin
      POSTGRES_PASSWORD: ${POSTGRES_WAREHOUSE_PASSWORD:?}
      ETL_LOADER_PW: ${POSTGRES_ETL_LOADER_PASSWORD:?}
      DBT_RUNNER_PW: ${POSTGRES_DBT_RUNNER_PASSWORD:?}
      SODA_READER_PW: ${POSTGRES_SODA_READER_PASSWORD:?}
      API_READER_PW: ${POSTGRES_API_READER_PASSWORD:?}
      GRAFANA_READER_PW: ${POSTGRES_GRAFANA_READER_PASSWORD:?}
      AIRFLOW_META_PW: ${POSTGRES_AIRFLOW_META_PASSWORD:?}
    command: >
      postgres
      -c shared_preload_libraries=pg_stat_statements
      -c shared_buffers=256MB
      -c max_connections=150
      -c log_connections=on -c log_disconnections=on
      -c log_min_duration_statement=1000
    volumes:
      - warehouse-db-data:/var/lib/postgresql/data
      - ./postgres/warehouse-init:/docker-entrypoint-initdb.d:ro
    networks: [data-net]
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U warehouse_admin -d warehouse"]
      interval: 10s
      timeout: 5s
      retries: 10
      start_period: 30s
    deploy: { resources: { limits: { cpus: "2.0", memory: 1G } } }
    logging: *default-logging
    security_opt: ["no-new-privileges:true"]

  # ------------------------------------------------------------ NEW: message broker (Valkey, Redis-compatible)
  valkey:
    image: ${VALKEY_IMAGE:-valkey/valkey:8}
    restart: unless-stopped
    environment:
      VALKEY_PASSWORD: ${VALKEY_PASSWORD:?}
    # --appendonly: queued tasks survive a broker restart
    # --maxmemory-policy noeviction: when full, refuse writes instead of silently dropping queued tasks
    command:
      - sh
      - -c
      - 'exec valkey-server --requirepass "$$VALKEY_PASSWORD" --appendonly yes --maxmemory 128mb --maxmemory-policy noeviction'
    volumes:
      - valkey-data:/data
    networks: [data-net]
    healthcheck:
      test: ["CMD-SHELL", 'valkey-cli --no-auth-warning -a "$$VALKEY_PASSWORD" ping | grep -q PONG']
      interval: 10s
      timeout: 5s
      retries: 10
      start_period: 5s
    deploy: { resources: { limits: { cpus: "0.5", memory: 192M } } }
    logging: *default-logging
    security_opt: ["no-new-privileges:true"]

  # ------------------------------------------------------------ object store (MinIO-compatible)
  minio:
    image: ${S3_IMAGE:?set S3_IMAGE in .env}
    restart: unless-stopped
    command: server /data --console-address ":9001"
    environment:
      MINIO_ROOT_USER: ${MINIO_ACCESS_KEY:?}
      MINIO_ROOT_PASSWORD: ${MINIO_SECRET_KEY:?}
    volumes:
      - minio-data:/data
    ports:                       # publishing needs a non-internal network, hence app-net
      - "127.0.0.1:9000:9000"
      - "127.0.0.1:9001:9001"
    networks: [data-net, app-net]
    healthcheck:
      test: ["CMD", "curl", "-fsS", "http://localhost:9000/minio/health/live"]
      interval: 10s
      timeout: 5s
      retries: 10
      start_period: 10s
    deploy: { resources: { limits: { cpus: "1.0", memory: 512M } } }
    logging: *default-logging
    security_opt: ["no-new-privileges:true"]

  minio-init:
    image: ${MC_IMAGE:?set MC_IMAGE in .env}
    restart: "no"
    depends_on:
      minio: { condition: service_healthy }
    environment:
      MINIO_ROOT_USER: ${MINIO_ACCESS_KEY:?}
      MINIO_ROOT_PASSWORD: ${MINIO_SECRET_KEY:?}
      MINIO_AIRFLOW_ACCESS_KEY: ${MINIO_AIRFLOW_ACCESS_KEY:?}
      MINIO_AIRFLOW_SECRET_KEY: ${MINIO_AIRFLOW_SECRET_KEY:?}
      MINIO_BACKUP_ACCESS_KEY: ${MINIO_BACKUP_ACCESS_KEY:?}
      MINIO_BACKUP_SECRET_KEY: ${MINIO_BACKUP_SECRET_KEY:?}
    entrypoint: ["/bin/sh", "/init.sh"]
    volumes:
      - ./minio/init.sh:/init.sh:ro
      - ./minio/policies:/policies:ro
    networks: [data-net]
    logging: *default-logging

  # ------------------------------------------------------------ airflow
  airflow-init:
    image: olist-airflow:${AIRFLOW_VERSION:-3.3.2}
    build:
      context: ../airflow
      args:
        AIRFLOW_VERSION: ${AIRFLOW_VERSION:-3.3.2}
        AIRFLOW_PYTHON: ${AIRFLOW_PYTHON:-3.12}
    user: "0:0"
    restart: "no"
    entrypoint: ["/bin/bash", "/init.sh"]
    environment:
      <<: *airflow-env
      _AIRFLOW_DB_MIGRATE: "true"
      AIRFLOW_ADMIN_USER: ${AIRFLOW_ADMIN_USER:?}
      AIRFLOW_ADMIN_PASSWORD: ${AIRFLOW_ADMIN_PASSWORD:?}
      AIRFLOW_UID: ${AIRFLOW_UID:-50000}
    volumes:
      - ../airflow/init.sh:/init.sh:ro
      - airflow-logs:/opt/airflow/logs
      - airflow-auth:/opt/airflow/auth
    networks: [data-net]
    depends_on:
      warehouse-db: { condition: service_healthy }
    logging: *default-logging

  airflow-apiserver:
    <<: *airflow-common
    restart: unless-stopped
    command: api-server
    volumes:                                       # CHANGED: only this service mounts the auth file
      - ../airflow/dags:/opt/airflow/dags:ro
      - ../airflow/plugins:/opt/airflow/plugins:ro
      - ../airflow/include:/opt/airflow/include:ro
      - airflow-logs:/opt/airflow/logs
      - airflow-auth:/opt/airflow/auth:ro
    ports:
      - "127.0.0.1:8080:8080"
    networks: [data-net, app-net]
    healthcheck:
      test: ["CMD", "curl", "--fail", "http://localhost:8080/api/v2/version"]
      interval: 30s
      timeout: 10s
      retries: 5
      start_period: 60s
    deploy: { resources: { limits: { cpus: "1.0", memory: 1G } } }

  airflow-scheduler:
    <<: *airflow-common
    restart: unless-stopped
    command: scheduler
    healthcheck:
      test: ["CMD", "curl", "--fail", "http://localhost:8974/health"]
      interval: 30s
      timeout: 10s
      retries: 5
      start_period: 60s
    deploy: { resources: { limits: { cpus: "1.5", memory: 1G } } }

  airflow-dag-processor:
    <<: *airflow-common
    restart: unless-stopped
    command: dag-processor
    healthcheck:
      test: ["CMD-SHELL", 'airflow jobs check --job-type DagProcessorJob --hostname "$${HOSTNAME}"']
      interval: 30s
      timeout: 30s
      retries: 5
      start_period: 60s
    deploy: { resources: { limits: { cpus: "1.0", memory: 1G } } }

  # ------------------------------------------------------------ NEW: Celery workers
  # General pool: 2 replicas x 4 slots. Listens on the default queue and on "ingest".
  airflow-worker:
    <<: *airflow-common
    restart: unless-stopped
    command: celery worker --queues default,ingest --concurrency 4
    environment:
      <<: *airflow-env
      DUMB_INIT_SETSID: "0"          # lets Celery shut down gracefully (finish running tasks)
    healthcheck:
      test:
        - CMD-SHELL
        - 'celery --app airflow.providers.celery.executors.celery_executor.app inspect ping -d "celery@$${HOSTNAME}" || celery --app airflow.executors.celery_executor.app inspect ping -d "celery@$${HOSTNAME}"'
      interval: 30s
      timeout: 15s
      retries: 5
      start_period: 60s
    deploy:
      replicas: 2
      resources: { limits: { cpus: "2.0", memory: 1536M } }

  # Heavy pool: 1 instance x 2 slots, queue "batch" only (reconciliation, backfills, dbt runs later).
  airflow-worker-batch:
    <<: *airflow-common
    restart: unless-stopped
    command: celery worker --queues batch --concurrency 2
    environment:
      <<: *airflow-env
      DUMB_INIT_SETSID: "0"
    healthcheck:
      test:
        - CMD-SHELL
        - 'celery --app airflow.providers.celery.executors.celery_executor.app inspect ping -d "celery@$${HOSTNAME}" || celery --app airflow.executors.celery_executor.app inspect ping -d "celery@$${HOSTNAME}"'
      interval: 30s
      timeout: 15s
      retries: 5
      start_period: 60s
    deploy: { resources: { limits: { cpus: "2.0", memory: 2G } } }

  # Optional Celery monitoring UI: start with `make up-monitoring`
  airflow-flower:
    <<: *airflow-common
    profiles: ["monitoring"]
    restart: unless-stopped
    command: celery flower
    environment:
      <<: *airflow-env
      AIRFLOW__CELERY__FLOWER_BASIC_AUTH: "${FLOWER_USER:?}:${FLOWER_PASSWORD:?}"
      FLOWER_USER: ${FLOWER_USER:?}
      FLOWER_PASSWORD: ${FLOWER_PASSWORD:?}
    ports:
      - "127.0.0.1:5555:5555"
    networks: [data-net, app-net]
    healthcheck:
      test: ["CMD-SHELL", 'curl --fail -u "$${FLOWER_USER}:$${FLOWER_PASSWORD}" http://localhost:5555/']
      interval: 30s
      timeout: 10s
      retries: 5
      start_period: 30s
    deploy: { resources: { limits: { cpus: "0.5", memory: 256M } } }

networks:
  data-net:
    internal: true        # no route to the internet
  app-net: {}

volumes:
  source-db-data:
  warehouse-db-data:
  minio-data:
  valkey-data:            # NEW
  airflow-logs:
  airflow-auth:
```

How the Celery pieces fit:

- **Queues:** a task without a `queue=` goes to `default` and is picked up by `airflow-worker`. Tasks marked `queue="ingest"` also go to that pool. Tasks marked `queue="batch"` go only to `airflow-worker-batch`. A task sent to a queue no worker listens on stays `queued` forever, which is the most common Celery mistake.
- **Slots:** 2 workers × 4 + 1 worker × 2 = 10 concurrent task slots. `parallelism=12` leaves headroom above that.
- **Scaling:** `make scale-workers N=3` adds a general worker. `deploy.replicas` sets the default of 2.
- **Fair distribution:** Airflow's default Celery prefetch multiplier is 1 and tasks are acknowledged late, so a worker takes a new task only when it frees a slot, and a task from a crashed worker is not silently lost.
- `airflow-init` is the only service with a `build:`; all Airflow services wait for it, so the image exists before they start.
- Workers have **no published ports**. They reach the API server's task-execution endpoint over `data-net`.
- `cap_drop`, read-only root filesystems, and image digests are left to **Phase 11**, because dropping capabilities from the Postgres image needs careful testing.
- Role passwords are visible in `docker inspect` of the Postgres containers. Acceptable locally; Phase 11 moves them to Docker secrets.

### Step 7b: Smoke DAG (proves multi-instance execution)

`airflow/dags/phase1_smoke_celery.py`:

```python
"""Phase 1 smoke test: fan out 12 tasks, record which worker ran each, check queue isolation."""
import socket
import time
from datetime import timedelta

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="phase1_smoke_celery",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(seconds=10)},
    tags=["phase1", "smoke"],
)
def phase1_smoke_celery():
    @task
    def numbers() -> list[int]:
        return list(range(12))

    @task(queue="default")
    def work(n: int) -> str:
        time.sleep(5)                 # long enough that slots stay busy and tasks spread out
        return socket.gethostname()   # the worker container's id

    @task(queue="batch")
    def on_batch_pool() -> str:
        return socket.gethostname()

    @task
    def verify(work_hosts, batch_host: str) -> None:
        distinct = set(work_hosts)
        print(f"general pool hosts: {sorted(distinct)}; batch pool host: {batch_host}")
        if len(distinct) < 2:
            raise ValueError("work was not spread across at least 2 workers")
        if batch_host in distinct:
            raise ValueError("batch queue task ran on a general worker")

    hosts = work.expand(n=numbers())
    verify(hosts, on_batch_pool())


phase1_smoke_celery()
```

`scripts/smoke-celery.sh` (triggers the DAG, waits, and checks the metadata DB):

```bash
#!/usr/bin/env bash
set -uo pipefail
cd "$(dirname "$0")/.."
set -a; . ./.env; set +a
C="docker compose --env-file .env -f infra/compose.yaml"
DAG=phase1_smoke_celery

q()  { $C exec -T -e PGPASSWORD="$POSTGRES_AIRFLOW_META_PASSWORD" warehouse-db \
         psql -h 127.0.0.1 -U airflow_meta -d airflow -tAc "$1"; }
af() { $C exec -T airflow-scheduler airflow "$@"; }

echo "waiting for the DAG processor to parse $DAG ..."
for _ in $(seq 1 36); do
  [ "$(q "select count(*) from dag where dag_id='$DAG'")" = "1" ] && break
  sleep 5
done
[ "$(q "select count(*) from dag where dag_id='$DAG'")" = "1" ] || { echo "FAIL  DAG never appeared"; exit 1; }

af dags unpause "$DAG" >/dev/null
af dags trigger "$DAG" >/dev/null

echo "waiting for the run to finish (up to 3 minutes) ..."
state=""
for _ in $(seq 1 36); do
  state=$(q "select state from dag_run where dag_id='$DAG' order by id desc limit 1")
  case "$state" in success|failed) break ;; esac
  sleep 5
done
rid=$(q "select run_id from dag_run where dag_id='$DAG' order by id desc limit 1")

[ "$state" = "success" ] && echo "PASS  DAG run succeeded" || { echo "FAIL  DAG state='$state'"; exit 1; }

hosts=$(q "select count(distinct hostname) from task_instance where dag_id='$DAG' and run_id='$rid' and task_id='work'")
[ "${hosts:-0}" -ge 2 ] && echo "PASS  'work' ran on $hosts different workers" || { echo "FAIL  'work' ran on $hosts worker(s)"; exit 1; }

echo; echo "tasks per worker:"
q "select hostname, count(*) from task_instance where dag_id='$DAG' and run_id='$rid' group by hostname order by 2 desc"
```

### Step 8: `Makefile`

Recipe lines must start with a **tab** character.

```make
COMPOSE := docker compose --env-file .env -f infra/compose.yaml
N ?= 3

.PHONY: secrets config build up up-monitoring down reset ps logs psql-src psql-wh check smoke scale-workers stats

secrets:
	@bash scripts/gen-secrets.sh

config:           ## validate compose + interpolation before starting anything
	@$(COMPOSE) config --quiet && echo "compose file OK"

build:
	@$(COMPOSE) build

up: config
	@$(COMPOSE) up -d --build --wait

up-monitoring: config   ## same as up, plus Flower on 127.0.0.1:5555
	@$(COMPOSE) --profile monitoring up -d --build --wait

down:
	@$(COMPOSE) --profile monitoring down

reset:            ## DESTROYS all data volumes
	@read -p "This deletes ALL volumes (databases, objects, broker). Continue? [y/N] " a && [ "$$a" = "y" ]
	@$(COMPOSE) --profile monitoring down -v --remove-orphans

ps:
	@$(COMPOSE) ps

logs:
	@$(COMPOSE) logs -f --tail=100

psql-src:
	@set -a; . ./.env; set +a; $(COMPOSE) exec -e PGPASSWORD="$$POSTGRES_SOURCE_PASSWORD" source-db psql -U source_admin -d source

psql-wh:
	@set -a; . ./.env; set +a; $(COMPOSE) exec -e PGPASSWORD="$$POSTGRES_WAREHOUSE_PASSWORD" warehouse-db psql -U warehouse_admin -d warehouse

check:
	@bash scripts/check-phase1.sh

smoke:            ## proves tasks spread across worker containers
	@bash scripts/smoke-celery.sh

scale-workers:    ## usage: make scale-workers N=3
	@$(COMPOSE) up -d --no-recreate --scale airflow-worker=$(N) airflow-worker

stats:
	@docker stats --no-stream --format "table {{.Name}}\t{{.MemUsage}}\t{{.CPUPerc}}"
```

If `up --wait` complains about the one-shot init containers on your Compose version, run `docker compose ... up -d --build` and then `make check`, which verifies health itself.

### Step 9: First boot

```bash
bash scripts/gen-secrets.sh      # fills .env (see Step 2)
# edit .env: S3_IMAGE, MC_IMAGE (and AIRFLOW_UID on Linux)
make config                       # catches missing variables early
time make up                      # first run builds the Airflow image and pulls images
make ps                           # expect 2 airflow-worker containers + 1 airflow-worker-batch
make check
make smoke
```

What to expect: the first `up` takes several minutes (image pulls and build). The Airflow UI is at `http://127.0.0.1:8080` (login: `AIRFLOW_ADMIN_USER` and the password from `.env`), and the object-store console at `http://127.0.0.1:9001`. After `make smoke`, open the DAG run in the Airflow UI and look at the task-instance details: the **hostname** differs between mapped `work` tasks.

### Step 10: Acceptance checks

`scripts/check-phase1.sh`:

```bash
#!/usr/bin/env bash
set -uo pipefail
cd "$(dirname "$0")/.."
set -a; . ./.env; set +a
C="docker compose --env-file .env -f infra/compose.yaml"
pass=0; fail=0
ok()  { echo "PASS  $1"; pass=$((pass+1)); }
bad() { echo "FAIL  $1"; fail=$((fail+1)); }

# run psql inside a database container as a given role: <container> <role> <password> <db> <sql>
sql() { $C exec -T -e PGPASSWORD="$3" "$1" psql -h 127.0.0.1 -U "$2" -d "$4" -tAc "$5" 2>&1; }
expect() {  # <description> <output> <pattern>
  if echo "$2" | grep -qiE "$3"; then ok "$1"; else bad "$1  (got: $(echo "$2" | head -n1))"; fi
}

echo "== 1. services healthy =="
for s in source-db warehouse-db valkey minio airflow-apiserver airflow-scheduler airflow-dag-processor; do
  st=$($C ps --format '{{.Health}}' "$s" 2>/dev/null | head -n1)
  [ "$st" = "healthy" ] && ok "$s healthy" || bad "$s health='$st'"
done
# replicated / pooled workers: <service> <expected healthy containers>
for pair in "airflow-worker 2" "airflow-worker-batch 1"; do
  set -- $pair
  n=$($C ps --format '{{.Health}}' "$1" 2>/dev/null | grep -c '^healthy$')
  [ "$n" -ge "$2" ] && ok "$1: $n healthy container(s)" || bad "$1: $n healthy, expected $2"
done

echo "== 2. network exposure =="
if docker ps --format '{{.Names}} {{.Ports}}' | grep '^olist-' | grep -qE '0\.0\.0\.0|:::'; then
  bad "a port is published on all interfaces"
else ok "all published ports are bound to localhost"; fi

echo "== 3. broker =="
expect "broker refuses unauthenticated clients" "$($C exec -T valkey valkey-cli ping 2>&1)" 'NOAUTH|Authentication required'

echo "== 4. warehouse least privilege =="
expect "api_reader can connect"             "$(sql warehouse-db api_reader "$POSTGRES_API_READER_PASSWORD" warehouse 'select 1')" '^1$'
expect "api_reader cannot read raw"         "$(sql warehouse-db api_reader "$POSTGRES_API_READER_PASSWORD" warehouse 'select 1 from raw.x')" 'permission denied'
expect "api_reader cannot read marts"       "$(sql warehouse-db api_reader "$POSTGRES_API_READER_PASSWORD" warehouse 'select 1 from marts.x')" 'permission denied'
expect "api_reader cannot create in api"    "$(sql warehouse-db api_reader "$POSTGRES_API_READER_PASSWORD" warehouse 'create table api.t(i int)')" 'permission denied|read-only'
expect "grafana_reader cannot read raw"     "$(sql warehouse-db grafana_reader "$POSTGRES_GRAFANA_READER_PASSWORD" warehouse 'select 1 from raw.x')" 'permission denied'
expect "airflow_meta cannot open warehouse" "$(sql warehouse-db airflow_meta "$POSTGRES_AIRFLOW_META_PASSWORD" warehouse 'select 1')" 'permission denied|not permitted'
expect "etl_loader can see audit schema"    "$(sql warehouse-db etl_loader "$POSTGRES_ETL_LOADER_PASSWORD" warehouse "select count(*) from information_schema.schemata where schema_name='audit'")" '^1$'
expect "warehouse allows 150 connections"   "$(sql warehouse-db warehouse_admin "$POSTGRES_WAREHOUSE_PASSWORD" warehouse 'show max_connections')" '^150$'

echo "== 5. source isolation =="
expect "extractor cannot read chaos"        "$(sql source-db extractor "$POSTGRES_SOURCE_EXTRACTOR_PASSWORD" source 'select 1 from chaos.x')" 'permission denied'
expect "extractor cannot write to shop"     "$(sql source-db extractor "$POSTGRES_SOURCE_EXTRACTOR_PASSWORD" source 'create table shop.t(i int)')" 'permission denied|read-only'

echo "== 6. object store policies =="
out=$($C run --rm --no-deps --entrypoint /bin/sh minio-init -c '
  mc alias set af http://minio:9000 "$MINIO_AIRFLOW_ACCESS_KEY" "$MINIO_AIRFLOW_SECRET_KEY" >/dev/null &&
  (echo ok | mc pipe af/quarantine/_check.txt >/dev/null 2>&1 && echo WRITE_QUARANTINE_OK || echo WRITE_QUARANTINE_DENIED) &&
  (echo no | mc pipe af/backups/_check.txt   >/dev/null 2>&1 && echo WRITE_BACKUPS_OK    || echo WRITE_BACKUPS_DENIED) &&
  (mc rm af/quarantine/_check.txt            >/dev/null 2>&1 && echo DELETE_OK           || echo DELETE_DENIED)
' 2>&1)
expect "airflow_svc can write quarantine"   "$out" 'WRITE_QUARANTINE_OK'
expect "airflow_svc cannot write backups"   "$out" 'WRITE_BACKUPS_DENIED'
expect "airflow_svc cannot delete objects"  "$out" 'DELETE_DENIED'

echo "== 7. workers do not hold the admin password file =="
expect "worker cannot read the auth file"   "$($C exec -T airflow-worker ls /opt/airflow/auth/passwords.json 2>&1)" 'No such file|cannot access'

echo "== 8. resource budget (caps total 10304 MiB, or 10560 MiB with Flower) =="
docker stats --no-stream --format "table {{.Name}}\t{{.MemUsage}}" | grep -E 'NAME|olist-'

echo; echo "passed=$pass failed=$fail"
[ "$fail" -eq 0 ]
```

(The test object left in `quarantine` is a versioned bucket's leftover `_check.txt` version; it is harmless.)

**Manual checks from the Phase 1 acceptance criteria:**

1. `make reset && time make up`: all services healthy in under 6 minutes (excluding the first image pulls; the extra minute is for the broker and workers).
2. `make stats`: total memory use under the caps in Section 0.2.
3. From another machine on your LAN, `curl http://<your-ip>:8080` should fail (localhost only).
4. A container with no internet: `docker compose --env-file .env -f infra/compose.yaml exec source-db sh -c 'getent hosts example.com'` returns nothing.
5. `make smoke` prints `PASS` for the run and for `'work' ran on N different workers` with N ≥ 2.

**Optional resilience drill** (preview of the Phase 8 chaos tests): run `make smoke`, and while the `work` tasks are running, `docker kill` one `airflow-worker` container. Airflow notices the missing task heartbeat only after `[scheduler] task_instance_heartbeat_timeout` (default 300 s), so for a quick demo set `AIRFLOW__SCHEDULER__TASK_INSTANCE_HEARTBEAT_TIMEOUT=60` temporarily. The affected tasks should then fail, be retried (the DAG has `retries=2`), and finish on the surviving worker. The timing and behaviour are the part I am least sure of, so treat this as an experiment.

---

## 5. Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| Init scripts never ran; roles missing | The data volume already existed; Postgres runs init scripts only on an empty data dir | `make reset`, then `make up` |
| `password authentication failed` after regenerating secrets | Old volume still holds the old passwords | `make reset`, or `ALTER ROLE` |
| `bad interpreter` or `\r` errors in `.sh` | CRLF line endings (Windows) | Add `.gitattributes` (Step 1), re-checkout; or `dos2unix` |
| `variable is not set` from Compose | Empty value in `.env` for a `:?` variable | Run `make secrets`; set `S3_IMAGE`, `MC_IMAGE` |
| `minio` unhealthy | Image lacks `curl`, or wrong command | Healthcheck `["CMD","mc","ready","local"]`, or a shell `/dev/tcp` check |
| `minio-init` fails on `policy` or `ilm` | `mc` version uses different subcommands | `mc admin policy --help`; adjust `init.sh` |
| `airflow-init` fails on `chown` or `Permission denied` on logs | `AIRFLOW_UID` mismatch (Linux) | Set `AIRFLOW_UID=$(id -u)`; `make reset`; `make up` |
| API server unhealthy; auth errors between components | `AIRFLOW_JWT_SECRET` or secret key differs between services | All services share one `.env`; check `docker compose config` |
| Login rejected | Auth file or option names differ in your Airflow build | Section 6, item 2 |
| **Tasks stay `queued` forever** | **No worker listens on the task's queue (typo in `queue=`), or the broker URL or password is wrong** | **`docker compose logs airflow-worker`; compare the DAG's `queue=` with `--queues`; check the broker password** |
| **Worker unhealthy (`inspect ping` fails)** | **Broker auth failure, or the Celery app path differs in your provider version** | **Look at the worker logs; the healthcheck tries both known paths** |
| **A task runs twice** | **Task runtime exceeded the broker's visibility timeout** | **Keep tasks shorter, or raise `...VISIBILITY_TIMEOUT` (set to 6 h here)** |
| **All tasks ran on one worker** | **Tasks too short, so one worker drained the queue** | **Use longer tasks (the smoke DAG sleeps 5 s); confirm 2 worker containers are healthy** |
| **Task logs missing in the UI for worker-run tasks** | **Log volume permissions, or the volume is not shared** | **Check that `airflow-logs` is mounted in the API server and workers, and `AIRFLOW_UID`** |
| **Worker killed (exit 137)** | **Out of memory** | **Lower `--concurrency` or raise the memory cap; check `make stats`** |
| **`too many clients` or connection-limit errors** | **More Celery or task connections than allowed** | **Raise `max_connections` or the role's `CONNECTION LIMIT`** |
| Containers killed or very slow | Docker has too little memory or CPU | Raise Docker's allocation (16 GB, 6 CPUs recommended) |
| `address already in use` | Another service on 8080, 9000, 9001, or 5555 | Change the host side of the port mapping |

---

## 6. Items I could not verify (check before relying on them)

1. **Object store image and `mc`:** the exact image name and tag of the MinIO-compatible fork (renamed in August 2026), whether it includes `mc`, and the `mc admin policy` and `mc ilm` syntax for your version.
2. **Airflow 3.3 auth settings:** I used the simple auth manager with a generated passwords file. Check the option names and file format in the 3.3.2 configuration reference. Best cross-check: download the official `docker-compose.yaml` for your exact Airflow version and diff the service commands, environment variables, and healthchecks against Step 7.
3. **Celery settings on 3.3.2:** the config keys (`[celery] broker_url` and `result_backend`, `[celery_broker_transport_options] visibility_timeout`, `[celery] flower_basic_auth`), the worker command `celery worker --queues ... --concurrency ...`, and the healthcheck app paths. The broker URL, result backend, worker command, and healthcheck pattern match the official Celery compose files I found (mostly from 2.x releases); the Airflow 3 specifics (task execution API URL, JWT secret) come from the earlier guide.
4. **Valkey image and CLI:** the `valkey/valkey:8` tag and the `valkey-cli` binary name. If the image only ships `redis-cli`, change the healthcheck.
5. **Metadata schema used by `make smoke`:** the `dag`, `dag_run`, and `task_instance` tables and the `task_instance.hostname` column. These should exist in 3.x, but check if the queries fail.
6. **`deploy.replicas` and `--scale`:** behaviour of replicas under plain Compose depends on your Compose version.
7. **PostgreSQL 18 support:** the Airflow 3.3.x range I found lists up to PostgreSQL 17.
8. **`docker compose up --wait` with one-shot containers** depends on your Compose version (fallback in Step 8).
9. **Heartbeat timeout drill:** the timing of failed-worker recovery.

---

## 7. Phase 1 done checklist

- [ ] `make reset && make up` leaves every long-running service healthy, including 2 `airflow-worker` containers and 1 `airflow-worker-batch`
- [ ] `make check` prints zero failures
- [ ] `make smoke` passes: tasks ran on at least 2 different workers, and the `batch` task ran on the batch worker
- [ ] Only localhost ports are published; database and broker ports are not
- [ ] Airflow UI login works; the metadata DB is the `airflow` database, not `warehouse`
- [ ] `.env` is gitignored; `.env.example` is committed; gitleaks passes
- [ ] ADRs written: (a) object-store choice and replaceability (1.1), (b) CeleryExecutor and its limits (1.3), (c) Valkey vs Redis vs RabbitMQ (1.3), (d) why Postgres 17, (e) why `data-net` is internal, (f) schema ownership and the permission matrix
- [ ] The permission matrix and the queue map (`default`, `ingest`, `batch`) are in `docs/architecture.md`
- [ ] Everything committed with a tag, e.g. `phase-1`

**Next:** Phase 2 builds the source schema (as the `emulator` role) and the emulator. When you build the Phase 3 ingest DAG, give the per-table extract tasks `queue="ingest"` and the reconciliation and backfill tasks `queue="batch"`.
