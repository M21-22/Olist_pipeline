# Olist Retail Data Platform — Implementation Plan

**Stack:** Docker Compose · Source PostgreSQL (fake operational DB) · Python emulator · **Airflow (CeleryExecutor, multiple workers) · Valkey (Celery broker)** · MinIO-compatible object store · Warehouse PostgreSQL · dbt · Great Expectations · Soda · FastAPI · Grafana (+ Prometheus)
**Dataset:** Olist Brazilian E-commerce (Kaggle, 9 CSVs, about 100k orders, 2016-2018)
**Pattern:** Orchestrated micro-batch ELT with incremental extraction from an operational source, executed by a pool of Celery workers
**Level target:** Strong mid-level data engineer

> **Naming:** "MinIO" in this plan means the S3-compatible object-store service named `minio` in Compose. It is currently a community fork of MinIO, because upstream MinIO is no longer distributed (5.7).

---

## Locked implementation decisions and precedence

This document is the **target architecture and implementation roadmap**, updated to reflect verified progress as of **2026-11-17**. It is **not** the operational source of truth for interfaces. Current repository code, SQL, Docker Compose, and executed test results take precedence over historical proposals here. A phase can be partially implemented before its complete acceptance criteria are met. Do not infer that a planned service, DAG, audit table, role, alert, or failure-injection harness exists merely because it appears in this plan.

- **Naming:** `source-db`, `warehouse-db`, `minio` (Silo image), `valkey`, `airflow-api-server`, `airflow-scheduler`, `airflow-dag-processor`, `airflow-worker-general-1`, `airflow-worker-general-2`, `airflow-worker-batch`. Source role `source_reader`; emulator role `emulator_writer`; Airflow metadata role `airflow_user`.
- **Configuration:** `.env.example` matches local `.env` variable names; `SIM_FROM` and `SIM_TO` are empty until explicitly set for a run. Never commit secret values.
- **Static source data:** `geolocation` and `product_category_translation` use one-time bootstrap snapshots and manual `refresh_reference` runs. Seven other source tables use timestamp-watermarked incremental extraction.
- **Watermarks:** independent landed and certified checkpoints; landed after manifest + audit registry, certified after Gate 2. Failed Gate 1/2 batches do not roll back landed progress; certification cannot skip a failed interval.
- **Airflow Python delivery:** Phase 3 packages reusable `airflow/include/` code in the Airflow image; no retroactive Phase 1 mount change.
- **Reference-state semantics:** each certified static snapshot replaces the current reference state in downstream staging; it is not unioned with prior snapshots.
- **Source delete and silent update recovery:** the implemented reconciliation compares source and certified raw state, but automated repair/tombstone generation is a **future Phase 7 objective**. Timestamp extraction alone does not capture these events.
- **Tests:** Phase 2 passed the 2017-03-01–2017-03-08 deterministic replay and restart tests. The ingestion DAG's seven mapped `land → certify` chains passed a normal manual run after the manual-run interval fix. The Docker-backed `make test` regression suite passed locally, including dedicated Gate 1 tests and isolated Gate 2 PostgreSQL integration. GitHub Actions execution and controlled single-table failure isolation have **not** yet been confirmed.
- **Actual loader signatures:** `_copy_raw(cursor, table, columns, copy_path)` and `_load_transaction(table, batch_id, interval_end, manifest, columns, copy_path)`. Never reintroduce unsupported `batch_id` or `parquet_key` parameters at these call sites.
- **Incremental window:** the implemented extraction boundary is `updated_at > start AND updated_at <= end` (with overlap applied when deriving the start). Manual DAG runs may have no `data_interval_end`; the ingestion task falls back to `dag_run.run_after`.
- **Reconciliation:** continuous (watermark-aware, lag-deferring) and full (strict, quiescent-source) modes exist and are tested against raw certified state. Automated repair/tombstone generation and the full Phase 7/Soda acceptance criteria remain future work.

---

## Current implementation status (2026-11-17)

| Phase | Current position | Evidence / remaining work |
|---|---|---|
| 0 — Decisions and tooling | Substantially implemented | Repository, ADR/defect-catalog foundations and development tooling established; verify remaining original acceptance items individually |
| 1 — Infrastructure | Core stack implemented | PostgreSQL, S3-compatible Silo (`minio` service), Valkey and Airflow CeleryExecutor are running; infrastructure smoke previously passed; not all hardening acceptance checks are certified |
| 2 — Emulator | One-week deterministic replay verified | 2017-03-01 to 2017-03-08, seed 42, repeatability and resume verified; full-history acceptance pending |
| 3 — Incremental extraction | Implemented and exercised | Seven mapped tables, landing, watermarks, manifest and manual-run handling; full fault-injection proof remains pending |
| 4 — Landing quality and raw certification | Implemented and exercised | Gate 1 / Gate 2, raw loading, audit and rollback semantics; tests pass locally; complete defect-catalog demonstration and alerts pending |
| 5–6 — dbt staging and marts | In development | dbt work is not yet considered accepted; do not infer completeness from files under `dbt/` |
| 7 — Gates 3–4 and reconciliation | Partially implemented ahead of phase | `reconcile` supports continuous and strict full modes; dbt/Soda gates, automatic repair and end-to-end acceptance remain pending |
| 8–11 — Chaos, API, observability, security/DR | Planned / partial foundations | No phase-level completion claim |
| 12 — CI/CD and documentation | Partial early implementation | Local isolated `make test` passes, GitHub Actions regression workflow exists; remote CI verification, DAG failure isolation and full release/demo checks remain pending |

This status table deliberately distinguishes **implemented**, **locally tested**, and **fully accepted**; it should be updated only after evidence is available.

---

## 0. Execution rules

- Phases 0–12 define the intended delivery order, steps, deliverables, and acceptance criteria. Small supporting infrastructure and tests may be developed earlier when required by already implemented phases; this does not mark later phases complete.
- Do not claim an entire phase complete until its own acceptance criteria have passed. Apply the repository's actual source interfaces rather than using this roadmap to reconstruct function signatures or resource names.

| Phase | Topic |
|---|---|
| 0 | Decisions, repo, tooling |
| 1 | Infrastructure foundation (incl. Celery workers and broker) |
| 2 | Source DB + emulator |
| 3 | Incremental extraction to landing |
| 4 | Gates 1-2, raw load, quarantine |
| 5 | dbt staging |
| 6 | dbt marts (star schema, SCD2) |
| 7 | Gates 3-4 (dbt tests, Soda, reconciliation) |
| 8 | Defect injection harness and detection proof |
| 9 | Serving API with contract |
| 10 | Observability (Grafana, alerting) |
| 11 | Security, backup, DR |
| 12 | CI/CD, docs, runbooks, demo |

---

## 1. Design principles and an honest statement about "catching all corrupted data"

No set of checks can *guarantee* detection of all corruption, because unknown failure modes exist. What this design does instead:

1. **A defect catalog (Section 6)** lists every known failure mode, the gate that catches it, and an automated test that injects it and asserts detection. Detection rate is measured and published, and the target is 100% of cataloged defects.
2. **Fail-closed policy.** Anything unrecognized is quarantined, not loaded. A new column, an unknown status value, or an unexpected file is an incident, not a silent pass.
3. **Source reconciliation as the ultimate backstop.** Row-hash comparison between the source DB and the warehouse catches silent corruption that no rule anticipated (changed values, missed updates, hard deletes).
4. **Layered gates**, each with a distinct job, so one tool's blind spot is covered by another.
5. **Idempotency everywhere.** Any interval or batch can be rerun or replayed from MinIO with identical results. **With Celery this also covers worker crashes, retries, and broker redelivery: any task may run twice, so every task must be safe to repeat.**

Other principles: least privilege, everything as code (dashboards, roles, DAGs, checks), local-first and cost-free by default, one-command startup, **replaceable dependencies (object store, broker) behind configuration**.

---

## 2. Target architecture

```
┌────────────────────┐   inserts/updates    ┌────────────────────┐
│ Emulator (Python)  │ ───────────────────► │ source-db (PG)     │  "operational DB"
│ replays Olist in   │                      │ orders, items,...  │  updated_at trigger
│ simulated time     │                      └─────────┬──────────┘
└────────────────────┘                                │ Airflow: incremental extract
                                                      │ (watermark + overlap window)
                                                      │ tasks run on Celery workers
                                                      ▼
                                   MinIO  landing/  (Parquet + manifest.json)
                                                      │
                        GATE 1: file/manifest/schema contract (Great Expectations)
                                    pass │            │ fail → copy to MinIO quarantine/ + alert
                                         ▼
                        Load to warehouse `raw` (COPY, idempotent per batch)
                        GATE 2: loaded rows == manifest rows; batch_registry=CERTIFIED
                                         ▼
 warehouse-db (PG)   raw → staging (dbt: type, dedupe, latest-state, reject routing)
                              │                 └► quarantine.rejected_rows
                        GATE 3: dbt tests + GE row-level rules on staging
                                         ▼
                           marts (dbt: facts, dims, SCD2)
                        GATE 4: dbt tests + Soda (freshness, volume, drift)
                                  + source↔warehouse reconciliation
                                         ▼
                           api schema (contract views, read-only role)
                                         ▼
                        FastAPI /v1 (OpenAPI contract, auth, rate limit)

 audit schema (batch_registry, dq_results, dbt_run_results, reconciliation, task_runs, api_log)
        ▼
 Grafana (dashboards + alerts)  ◄── Prometheus (API/Airflow metrics)

 Orchestration plane:  Airflow scheduler ──► Valkey queues ──► Celery workers
                         default, ingest ─► airflow-worker ×2 (4 slots each)
                         batch           ─► airflow-worker-batch ×1 (2 slots)
```

### 2.1 Services (Docker Compose)

| Service | Purpose | Notes | Memory cap (MiB) |
|---|---|---|---|
| `source-db` | Fake operational PostgreSQL | Separate container for realism | 384 |
| `warehouse-db` | Warehouse + Airflow metadata + Celery results (separate databases, separate users) | One server saves RAM; `max_connections=150` | 1024 |
| `minio` + `minio-init` | Object storage, bucket and policy bootstrap | Versioning on; MinIO-compatible fork (5.7) | 512 (init is one-shot) |
| **`valkey`** | **Celery message broker** | **Password, AOF persistence, `noeviction`, internal network only** | **192** |
| **`airflow-api-server`, `airflow-scheduler`, `airflow-dag-processor`** | **Airflow 3.3.2 control plane** | **CeleryExecutor; triggerer not included until deferrable operators are used** | **1024 each (3072)** |
| **`airflow-worker-general-1`, `airflow-worker-general-2`** | **General Celery pool, queues `default` and `ingest`** | **4 slots each; scale with `make scale-workers N=3`** | **1536 each (3072)** |
| **`airflow-worker-batch`** | **Heavy Celery pool, queue `batch`** | **2 slots; runs dbt, Soda, reconciliation, backup, replay** | **2048 now; 3072 from Phase 5** |
| **`airflow-flower` (optional profile `monitoring`)** | **Celery monitoring UI** | **Localhost only, basic auth** | **256** |
| `emulator` | Drives the source DB | Own container, profile-controlled | 256 |
| ~~`dbt`~~ | ~~dbt-postgres runner image~~ | **Removed as a separate service: dbt runs in an isolated virtualenv inside the worker image (5.2)** | **0** |
| `api` | FastAPI app | Non-root, read-only role | 256 |
| `grafana` | Dashboards and alerting | Provisioned from files | 256 |
| `prometheus` + `statsd-exporter` (optional) | Metrics for Airflow and the API | Small footprint | 256 + 64 |
| `mailpit` | Local SMTP sink for alert emails | Dev only | 64 |

**Resource target: Docker allocation 16 GB and at least 6 CPUs; worst-case container caps total 12736 MiB (12.4375 GiB).** Set `deploy.resources.limits` (memory and CPUs) on every service. Caps are ceilings: actual use is normally well below them, so measure with `make stats`.

### 2.2 Warehouse schemas and roles

| Schema | Content | Written by |
|---|---|---|
| `raw` | Append-only as-landed tables + `_batch_id`, `_loaded_at`, `_source_file`, `_row_hash` | `etl_loader` |
| `staging` | Typed, deduplicated, latest-state, normalized | `dbt_runner` |
| **`quarantine`** | **`rejected_rows` (row-level reason codes). Batch-level quarantine records live in `audit.batch_registry` and the MinIO `quarantine` bucket.** | **`dbt_runner`** |
| `snapshots` | dbt snapshots (SCD2 source) | `dbt_runner` |
| `marts` | Facts and dimensions | `dbt_runner` |
| `api` | Stable contract views over marts | `dbt_runner` (owner), `api_reader` (SELECT) |
| `audit` | Batch registry, DQ results, run metadata, reconciliation, **task runs (which worker ran what)**, API log | various |

Roles: `etl_loader`, `dbt_runner`, `soda_reader`, `api_reader` (SELECT on `api` only), `grafana_reader` (SELECT on `audit`, `quarantine`, and `api`), `airflow_user` (Airflow metadata database; no warehouse data privileges). No application role is a superuser. Source side: `emulator_writer` (writes `shop`) and `source_reader` (read-only on `shop`, **no access to the `chaos` schema**). Connection limits and access controls must be verified against the PostgreSQL initialization SQL and Compose configuration.

---

## 3. Repository layout

```
olist-platform/
├── README.md
├── Makefile                      # infrastructure commands and isolated regression targets (`make test`, etc.)
├── .env.example                  # never commit .env
├── .gitattributes                # NEW: force LF endings for .sh/.sql/.yaml/Makefile
├── .pre-commit-config.yaml
├── scripts/                      # NEW: gen-secrets.sh, check-phase1.sh, smoke-celery.sh
├── docs/
│   ├── architecture.md           # planned: permission matrix, queue map
│   ├── adr/                      # 0001-micro-batch.md, 0002-object-store.md, 0003-celery.md, ...
│   ├── runbooks/                 # one per alert
│   ├── data-contracts/           # one YAML per exposed mart
│   └── defect-catalog.md
├── infra/
│   ├── compose.yaml
│   ├── compose.override.yaml     # dev ports, mailpit
│   ├── compose.test.yaml         # implemented: disposable PostgreSQL test stack and test runner
│   ├── postgres/{source-init,warehouse-init}/   # CHANGED: roles, schemas, grants, extensions
│   ├── minio/                    # init.sh, policies/*.json, optional Dockerfile.mc
│   ├── grafana/provisioning/     # datasources, dashboards, alert rules
│   └── prometheus/
├── emulator/                     # Python package + Dockerfile
├── airflow/
│   ├── dags/  plugins/  include/ (extract, load, reconcile, dq helpers)
│   ├── Dockerfile                # CHANGED: constraints-pinned deps + isolated tool venvs (dbt, soda, GE if needed)
│   ├── requirements.txt
│   └── init.sh                   # NEW: auth file, permissions, DB migrate
├── dbt/                          # models/{staging,marts,api}, snapshots, tests, macros, seeds
├── quality/
│   ├── great_expectations/       # gx project: suites, checkpoints
│   └── soda/                     # configuration.yml, checks/*.yml
├── api/                          # FastAPI app, tests, openapi.json (committed)
├── chaos/                        # defect injectors + detection-matrix tests
├── tests/                        # unit + PostgreSQL integration regression tests; pytest.ini
└── .github/workflows/            # regression.yml implemented; further workflows planned
```

---

## 4. Phased build

### Phase 0: Decisions, repo, tooling

**Steps**
1. Create the repo and the layout above. Add `.gitignore` (`.env`, data, `target/`, `logs/`) **and `.gitattributes` (LF line endings; CRLF breaks shell scripts in containers).**
2. Install pre-commit hooks: ruff, sqlfluff (Postgres dialect), yamllint, gitleaks, end-of-file/trailing-whitespace.
3. Write the first ADRs: micro-batch vs streaming, **object-store choice and replaceability (5.7)**, one Postgres server with separate DBs, **CeleryExecutor and its limits (5.6)**, **broker choice: Valkey vs Redis vs RabbitMQ (5.6)**, **Postgres 17**, **internal data network**, GE/Soda/dbt responsibility split (Section 5.4).
4. Download Olist from Kaggle (needs a free account) and check its license terms. Keep CSVs out of git; document the download step in the README.
5. Draft the **defect catalog** (Section 6) as `docs/defect-catalog.md`. It is the spec for Phases 4-8.
6. Record the actual pinned Compose image tags and verify the S3 client and Airflow 3.3.2 CLI/API behavior in the existing Phase 1 smoke tests.

**Deliverables:** repo skeleton, ADRs 1-7, defect catalog draft, Makefile stubs, verified pin list.
**Acceptance:** `pre-commit run --all-files` passes; a teammate can read the ADRs and explain why the project is micro-batch, why it uses Celery, and why the object store is a fork.

---

### Phase 1: Infrastructure foundation

The steps and acceptance criteria below define this phase; verify implemented settings against the repository configuration.

**Steps**
1. Write `compose.yaml` with named volumes, two networks (**`data-net` marked `internal: true`, so data services have no internet route; `app-net` for published ports**), healthchecks on every service, `depends_on` with `service_healthy` / `service_completed_successfully`, resource limits, restart policies, and log rotation.
2. Bind published ports to `127.0.0.1` only. **Database and broker ports are not published** (use `make psql-wh` / `make psql-src`).
3. `postgres/source-init` and `postgres/warehouse-init`: create databases (`source`, `warehouse`, `airflow`), roles with connection limits, schemas, grants and default privileges (Section 2.2), role guard rails (read-only, statement timeouts), and extensions (`pgcrypto`, `pg_stat_statements`).
4. Object-store bootstrap: buckets `landing`, `quarantine`, `backups`; versioning on **with a 30-day expiry of old versions**; service accounts with bucket-scoped policies (**Airflow: read/write on `landing` and `quarantine`, no delete, so landing is immutable; backup job: read/write/delete on `backups`, read-only on `landing`**).
5. **Valkey broker** with password, AOF persistence, memory cap, and `noeviction`, on `data-net` only.
6. **Airflow 3.3.2 with CeleryExecutor:** API server, scheduler, DAG processor; **2 general workers (queues `default`, `ingest`) and 1 batch worker (queue `batch`)**; optional Flower; result backend in the metadata DB; connections provided as `AIRFLOW_CONN_*` environment variables (not stored in the metadata DB); the admin-password file is mounted into the API server only; example DAGs off.
7. Secrets: `.env` for local, generated by `make secrets` (random hex, never defaults; a re-run never overwrites existing values).
8. **Smoke DAG `infrastructure_smoke_test` and `make smoke`:** 12 mapped tasks, asserts they ran on at least 2 different workers and that the `batch`-queue task ran on the batch worker.
9. Add `make up`, `up-monitoring`, `down`, `reset`, `ps`, `logs`, `check`, `smoke`, `scale-workers`, `stats`.

**Deliverables:** one-command stack, healthy services, bootstrap SQL and object-store policies in git, `check-phase1.sh`, smoke DAG.
**Acceptance:**
- `make reset && make up` yields all long-running services healthy (10 containers, 11 with Flower) in under **6** minutes, excluding first image pulls.
- `make check` reports zero failures, including: `api_reader` cannot read `raw`; `airflow_user` cannot open `warehouse`; `source_reader` cannot read `chaos`; the broker rejects unauthenticated clients; workers cannot read the admin-password file; the Airflow service account cannot delete objects or write to `backups`.
- `make smoke` passes with tasks on at least 2 general workers.
- `make stats` shows total memory under the caps in 2.1; no service published beyond localhost.

---

### Phase 2: Source DB and emulator

**Goal:** a believable operational system that changes over time.

**Steps**
1. **Source schema** (`source` DB, schema `shop`, **tables created as the `emulator_writer` role so the extractor's default privileges apply**): `customers`, `orders`, `order_items`, `order_payments`, `order_reviews`, `products`, `sellers`, `product_category_translation`; geolocation as static reference data. Each mutable table gets `updated_at timestamptz` maintained by a `BEFORE UPDATE` trigger, plus primary keys.
2. **Emulator** (Python, `psycopg`, seeded RNG):
   - Simulated clock: `SIM_SPEED` (e.g. 1 simulated day per 5 real minutes) and `--backfill` mode.
   - Event generation from Olist timestamps: order **inserted** at purchase time; **updated** at approval, carrier handover, delivery, and cancellation; items and payments inserted with the order; reviews inserted at creation and **updated** when answered.
   - Master data seeded upfront, except a configurable fraction of products and sellers inserted **after** the items that reference them (orphan scenario).
   - Writes in small transactions, with `updated_at` stamped by the DB.
3. **Source-side defect modes** (all seeded and individually switchable via config):
   - *Silent update:* change a row with the trigger bypassed, so `updated_at` does not move.
   - *Backdated update:* set `updated_at` earlier than the extraction watermark.
   - *Hard delete:* delete rows outright (a few orders or reviews).
   - *Bad values:* negative price/freight, review score outside 1-5, future timestamps, impossible timestamp order, invalid state codes, unknown order status, whitespace and casing noise in text.
   - *Duplicate business keys:* simulate an application bug that inserts a duplicate item line.
4. Emulator writes a **ground-truth log** (`chaos.injected_defects`, in a separate schema of the source DB that **the extractor role cannot read**): defect type, table, key, time. This is what the Phase 8 harness compares against detections.
5. Emulator is idempotent on restart (resumes from a stored simulated-time cursor).

**Deliverables:** emulator container, source DDL, config reference, ground-truth log.
**Acceptance:** after a full run, source row counts match Olist (excluding intentionally deleted rows); the ground-truth log lists every injected defect; re-running with the same seed reproduces identical data.

---

### Phase 3: Incremental extraction to landing

**Implementation and target:** Source reads use `source_reader` on `source.shop`; extraction state and batch audits live under warehouse `audit`. The `ingest` DAG uses the `ingest` queue, a 15-minute cadence, `max_active_runs=1`, and a 10-minute overlap, with UTC intervals. The original proposal of a dedicated `ingest_audit_writer` role must not be treated as an existing role without checking current grants and connection configuration. The actual deployed audit role and connection IDs are defined by code/Compose, not by this text. The DAG now uses a mapped per-table task group, preserving each table's `land → certify` dependency.

**Steps**
1. Create `audit.extraction_watermarks` (`table_name` primary key, `landed_high_watermark timestamptz`, `last_landed_batch_id`, `updated_at`), `audit.landed_batches` (unique table + logical interval + batch ID, object keys, status, published timestamp), and `audit.task_runs` (DAG, task, map index, batch ID, try number, hostname, start/end/status). The audit writer receives only required table privileges; the source reader remains read-only and has no `chaos` access.
2. Implemented `ingest` with dynamic mapping of an independent `land → certify` group for each of **seven mutable tables**: `customers`, `orders`, `order_items`, `order_payments`, `order_reviews`, `products`, `sellers`. Query `updated_at > landed_high_watermark - interval '10 minutes' AND updated_at <= fixed_interval_end` in one `REPEATABLE READ READ ONLY` transaction. Capture counts, control totals and null counts from the same snapshot. Use an Airflow source pool of **8** slots (below the 10-connection source reader cap).
3. **Static reference tables:** load `geolocation` and `product_category_translation` once as complete snapshots on initial bootstrap. Refresh only by an explicitly triggered `refresh_reference` DAG, not every 15 minutes. Use deterministic snapshot batch IDs derived from table + requested logical snapshot identifier; each refresh replaces the downstream current reference state after certification, not append-as-new-business-rows.
4. Generate canonical, byte-stable Parquet and a canonical JSON manifest with table, deterministic batch ID, extraction interval, row count, source control totals, min/max `updated_at` (null for reference snapshots), per-column null counts, SHA-256, schema fingerprint, extractor version, and extraction mode. Sort rows by primary key; fix schema, compression, row-group size and serialization settings. No runtime timestamps in the canonical manifest; operational timestamps belong in audit tables.
5. Upload `landing/<table>/dt=YYYY-MM-DD/hour=HH/<batch_id>.parquet`, verify upload, then publish `<batch_id>.manifest.json` **last**. An object without a manifest is incomplete. In a single warehouse transaction, register the landed batch and advance the landed watermark monotonically. On crash after manifest but before audit commit, the retry reconciles the manifest and commits the same batch; on crash before manifest, retry overwrites the same deterministic key. S3 versioning preserves previous versions.
6. Configure task retries with exponential backoff (3 retries; 1, 2 and 4 minutes), execution timeout 10 minutes, source connection timeout 10 seconds, and a heartbeat warning after **4** consecutive zero-row intervals while the emulator is confirmed active. Log every attempt in `audit.task_runs`.
7. Package reusable extraction code from `airflow/include/` into the Airflow image. Build the image and verify imports in the DAG processor and both general Celery workers. No separate bind mount for `include/` is required. Use source/S3/audit credentials from environment or Airflow connections without exposing secrets in logs.

**Checkpoint policy:** Phase 3 advances only the **landed checkpoint** after durable publication and audit registration. Phase 4 creates and advances the separate **certified checkpoint** after Gate 1, transactional raw load and Gate 2. Certification is per-table ordered and gap-free; quarantined batches block certification of later intervals for that table until resolved. The extractor continues landing new intervals independently. Silent/backdated updates and hard deletes are handled by Phase 7 reconciliation, not claimed to be detectable from `updated_at` alone.

**Deliverables (mixed status):** `ingest` DAG, reusable packaged extraction code, audit migration and unit/integration tests are implemented. `refresh_reference` and the full fault-injection acceptance matrix must be verified separately; this line does not certify them.

**Acceptance:** same logical interval produces identical Parquet bytes and canonical manifest; incomplete upload has no manifest/checkpoint advancement; retry after manifest publication recovers without a duplicate logical batch; all eligible timestamped source rows are covered; static snapshots load without `updated_at`; task audit proves execution on both general workers.

---

### Phase 4: Gates 1-2, raw load, quarantine

**Implementation note:** Gate 1 and Gate 2 run as part of each table's `land → certify` chain. Dedicated Gate 1 unit tests, Gate 2 unit/integration tests, and an isolated local Docker regression suite passed. The policies below remain the intended complete acceptance contract; do not assume every planned alert, row-level rejection route or chaos injection is implemented.

**Gate 1: landing validation (before raw data is loaded into the warehouse)**
1. Manifest present and parseable; checksum matches; Parquet readable end to end; row count equals the manifest.
2. **Schema contract** via a Great Expectations suite per table: exact column set, types, nullability, key uniqueness within the batch, value ranges, allowed categorical values, timestamp sanity.
3. **Schema-drift policy**:
   - Added nullable column: warn, record, land the file, and open a ticket-style alert. Do not load the new column until the contract is updated.
   - Removed, renamed, or type-changed column: **fail and quarantine**.
4. Failure action: **copy** the batch to `quarantine/<table>/<batch_id>/` with `reason.json` (landing stays untouched), mark `audit.batch_registry.status = QUARANTINED`, alert, and **do not advance the certified checkpoint**. The landed checkpoint is not rolled back; repair and replay use the already-landed immutable batch.

**Load**
5. `COPY` into `raw.<table>` inside one transaction per batch, with `_batch_id`, `_loaded_at`, `_source_file`, `_row_hash`. Reload of the same batch deletes then inserts that `_batch_id` within the transaction. `batch_registry` has a unique key on batch id, so replays and **redelivered tasks** cannot double-load. **Per-table validate and load tasks run on `queue="ingest"`** (the loader's connection limit is 30).

**Gate 2: load reconciliation**
6. After load: `count(*) where _batch_id = X` must equal the manifest row count; sums of key numeric columns must match the manifest control totals. Only then set status `CERTIFIED`.

**Quarantine policy**
7. Batch-level problems go to the MinIO quarantine bucket (a copy) and `audit.batch_registry`. Row-level problems (found by GE in `unexpected_index_list`) are loaded with a flag and routed in staging to `quarantine.rejected_rows`. Policy: structural/manifest/schema errors fail the entire batch; business-value errors are recorded as rejected rows in staging, except required identity/primary-key nulls, which fail the batch.
8. Write all gate results to `audit.dq_results` (gate, suite/check, table, batch, status, failed count, timestamp).
9. **Keep validation memory-bounded:** a general worker has a 1536 MiB cap, so validate batches in chunks or via Parquet row groups rather than loading a whole batch into one dataframe if batches grow.

**Deliverables:** GE project with suites and checkpoints, loader, `batch_registry`, quarantine paths, runbook "quarantined batch".
**Acceptance:** injecting each Gate 1/2 defect (D03-D07, D22) produces the expected status, a quarantine object, an alert, and an `audit.dq_results` row, with no data in `raw`.

---

### Phase 5: dbt staging

**Steps**
1. **Add the dbt virtualenv layer to the Airflow image** (see 5.2) and raise the batch worker's memory cap from 2048 to 3072 MiB. dbt tasks run on `queue="batch"` and connect as `dbt_runner`.
2. dbt project with `sources` for `raw` (including **source freshness** on `_loaded_at`).
3. Staging models read only batches with `batch_registry.status = 'CERTIFIED'`. Pattern: `raw` joined to `audit.batch_registry`.
4. Per model:
   - cast and rename types, normalize text (trim, case, state code uppercase), parse timestamps;
   - **dedupe** by business key, keeping the latest by (`updated_at`, then `_loaded_at`);
   - **status monotonicity:** for orders, the latest valid state wins; an older snapshot arriving later must not overwrite a newer one;
   - **reject routing:** rows failing value rules go to `quarantine.rejected_rows` with a reason code, and are excluded from `stg_*`.
5. Use PostgreSQL dbt incremental `delete+insert` with an explicit `unique_key` and 24-hour lookback; older corrections are processed through reconciliation repair batches. Record this fixed policy in an ADR.
6. Handle **hard deletes**: a `staging_deleted_keys` model fed by the reconciliation job (Phase 7), applying soft deletion flags.
7. Enable **model contracts** (`contract: {enforced: true}`) on all staging models.

**Deliverables:** `stg_orders`, `stg_order_items`, `stg_payments`, `stg_reviews`, `stg_customers`, `stg_products`, `stg_sellers`, `stg_geolocation`, `rejected_rows`.
**Acceptance:** staging contains exactly one row per business key; replaying the same batches twice leaves staging unchanged; every injected bad-value row appears in `quarantine.rejected_rows` with the right reason code.

---

### Phase 6: dbt marts

**Star schema**

| Model | Grain / type |
|---|---|
| `fct_orders` | One order: milestone timestamps, delivery delay vs estimate, payment totals, status |
| `fct_order_items` | One order item: price, freight, keys to all dimensions |
| `fct_payments` | One payment row |
| `fct_reviews` | One review |
| `dim_customer` | **SCD2** via dbt snapshot using the `check` strategy on address-related attributes (`customer_unique_id` mapped across `customer_id`s) |
| `dim_product`, `dim_seller` | Type 1, with category translation |
| `dim_geography` | Zip-prefix level, deduplicated from the 1M-row geolocation table |
| `dim_date` | Generated calendar |

**Steps**
1. Surrogate keys via `dbt_utils.generate_surrogate_key`; an `unknown` member row per dimension for orphans (inferred members), with a flag so late-arriving dimension rows can replace them.
2. Facts are incremental with lookback; SCD2 via `dbt snapshot` with the `check` strategy on tracked customer attributes; snapshot runs on the `batch` queue.
3. Analytics models for the end-of-pipeline use cases:
   - `mart_seller_delivery_sla`: rolling baseline and z-score of delay vs estimate by seller and region, with an alert flag (this feeds the API and Grafana).
   - `mart_daily_kpis`: orders, GMV, average review score, cancel rate.
4. Contract enforcement and documentation (descriptions on every column), `dbt docs generate`.
5. **`api` schema views:** stable, explicitly column-listed views over marts. They are the only thing the API role can read, and they decouple the public contract from internal refactors.

**Deliverables:** marts, snapshot, `api` views, dbt docs.
**Acceptance:** fact grains verified by uniqueness tests; SCD2 validity ranges never overlap and have exactly one current row per natural key; the `unknown` member absorbs orphans without dropping fact rows.

---

### Phase 7: Gates 3-4 (dbt tests, Soda, reconciliation)

**Gate 3: staging (logic and integrity)**
- dbt generic tests: `unique`, `not_null`, `accepted_values`, `relationships` (with severity per rule).
- dbt singular tests: timestamp ordering (purchase ≤ approved ≤ carrier ≤ delivered), non-negative money, status/timestamp consistency (a `delivered` order must have a delivery timestamp).
- Unit tests (dbt unit tests) for the tricky logic: dedupe order, late-update precedence, SCD2 transitions.

**Gate 4: marts (SLA, volume, drift) with Soda**
- Freshness (max `_loaded_at` or event time age) per mart.
- Volume checks against a **baseline computed from `audit.metrics_history`** (rolling mean and tolerance), so the checks do not depend on any Soda paid feature.
- Distribution checks: missing percent, duplicate count, min/max of money columns, share of `unknown` dimension members, cancel-rate bounds.
- Cross-table consistency: item count per order vs orders; fact totals vs payment totals within a calibrated tolerance. Olist has natural inconsistencies, so **profile the clean data first and set thresholds from observed baselines**, not from assumptions.

**Reconciliation (implemented foundation; remaining roadmap)**
- **Implemented now:** source-versus-**certified raw state** reconciliation with `continuous` mode (compares data only through the certified watermark, defers newer source changes and certain warehouse-only keys whose deletion time is unknown) and `full` mode (strict comparison after source writes stop and ingestion catches up). Both persist outcomes in `audit.reconciliation`. The one-week orders scenario returned `MATCH`, and boundary/lag-mode regressions were exercised.
- **Not implemented as a verified end-to-end feature:** automatic repair/tombstone batches, bucketed aggregation of source-versus-**staging** hashes, full mismatch drill-down/alerting, and Soda assertions over the reconciliation results. These remain target enhancements once staging and marts exist.
- The future repair path re-extracts affected keys into validated repair batches (or produces deletion tombstones) and alerts when recovery fails. Never assume a `continuous` `DEFERRED` result means corruption, or that a strict `full` comparison is valid while the source is actively mutating.

**Publish rule:** the `transform` DAG runs `dbt build` (so a failing test stops downstream models), then Soda; on failure it sets `audit.serving_state` to `DEGRADED` or `BLOCKED` by severity. The API reads this state (Phase 9).

**Deliverables:** test suites, Soda checks, reconciliation job, repair job, severity matrix (which failures block, which warn).
**Acceptance:** all Gate 3-4 defects (D08-D21) are detected by the intended check; reconciliation finds injected silent updates and hard deletes and repairs or flags them.

---

### Phase 8: Defect injection harness and detection proof

**Steps**
1. `chaos/` contains one injector per catalog entry. Each is deterministic (seeded) and scoped, e.g. "truncate the Parquet file for `orders` batch N" or "negate price on 5 rows". **Infrastructure injectors (D23-D25) kill a worker, restart the broker, or shorten the visibility timeout.**
2. Test pattern (pytest, runs against the Compose stack):
   - start from a known clean state;
   - apply one injector;
   - trigger the relevant DAG(s);
   - assert: the **expected gate** failed or flagged it, the **expected audit rows** exist, the **bad data never reached `marts`/`api`**, and the alert fired.
3. Compare the emulator's ground-truth log with `audit.dq_results` and `quarantine.rejected_rows`. **Detection rate = detected / injected**, per defect class. Publish it as a table in the README and a Grafana panel.
4. Also test **no false positives**: a clean full run produces zero failures.
5. Add a **random-fuzz job** (small, seeded) that applies random combinations of defects, to probe for gaps beyond the catalog. Any undetected case becomes a new catalog entry and a new check.
6. **Resilience drills (D23-D25):** for each, assert the pipeline ends in the same final state as a clean run (same row counts and hashes), with the expected retries recorded in `audit.task_runs`.

**Deliverables:** harness, detection matrix, CI job running the matrix on a small dataset slice.
**Acceptance:** 100% detection on the catalog, 0 false positives on clean runs, **all resilience drills end in the clean-run state**, fuzz job finds nothing new (or each finding is converted into a catalog entry and fixed).

---

### Phase 9: Serving API (FastAPI) with a documented contract

**Endpoints (all under `/v1`)**

| Endpoint | Purpose |
|---|---|
| `GET /health` | Liveness (no auth) |
| `GET /status` | Data freshness, `serving_state`, last certified batch, last successful transform |
| `GET /orders/{order_id}` | One order with items summary, payments, review score |
| `GET /orders` | Filters: status, date range, customer state; cursor pagination |
| `GET /sellers/{seller_id}/delivery-performance` | Delay stats and z-score history |
| `GET /sla/alerts` | Sellers or regions currently beyond their baseline |
| `GET /kpis/daily` | Daily KPI series |
| `GET /categories/summary` | Category-level sales summary |

**Contract**
1. **OpenAPI 3.1 as the contract**, exported to `api/openapi.json` and committed. Pydantic response models with descriptions and examples. Errors follow RFC 7807 problem details.
2. **Data contract per exposed mart** in `docs/data-contracts/*.yaml`: owner, description, schema, freshness SLA, quality guarantees, versioning and deprecation policy. The dbt contract on the `api` views enforces the schema side.
3. **Versioning rule:** additive changes stay in `/v1`; breaking changes require `/v2` and a deprecation window (use `Deprecation`/`Sunset` headers). CI runs `oasdiff` (or equivalent) against the committed spec and fails on breaking changes.
4. **Data-state awareness:** responses carry `X-Data-Freshness` and `X-Data-Status` headers. `BLOCKED` returns 503 for affected endpoints; `DEGRADED` returns data with a warning header.

**Security and robustness**
5. API-key auth (keys stored hashed, with scopes), per-key rate limiting, request size limits, CORS off by default, strict input validation, parameterized queries only.
6. DB access via `api_reader` on the `api` schema only; connection pool with timeouts and a statement timeout; paginated queries only, with a hard `limit` cap.
7. Metrics (`prometheus-fastapi-instrumentator`), structured JSON logs, and a request log to `audit.api_log` (no secrets or full keys).
8. Non-root container, read-only filesystem where possible, health and readiness probes.

**Tests:** unit tests; **contract tests with Schemathesis** against the running API; auth and permission tests; test that no PII-like fields outside the allowlist ever appear in responses.

**Deliverables:** API service, committed OpenAPI spec, data-contract YAMLs, contract tests.
**Acceptance:** Schemathesis passes; the API role cannot query `raw`/`staging`; a simulated `BLOCKED` state yields 503s; breaking a field in a view fails CI.

---

### Phase 10: Observability (Grafana, Prometheus, alerting)

**Data sources:** Postgres (`grafana_reader` on `audit`/`quarantine`/`api`), Prometheus (Airflow StatsD metrics, API metrics).

**Dashboards (provisioned as JSON/YAML in git)**
1. **Pipeline health:** DAG success rate and duration trends, task retries, extraction lag, batch counts by status.
2. **Data quality:** gate pass/fail by layer and table, quarantined batches, rejected rows by reason, **detection rate vs injected defects**.
3. **Freshness and SLA:** age of each mart vs its SLA, watermark lag per table.
4. **Reconciliation:** source vs warehouse mismatches over time, repair outcomes.
5. **dbt:** model runtimes, row counts (parsed from `run_results.json` into `audit.dbt_run_results`), test results.
6. **API:** request rate, latency percentiles, error rates, per-key usage.
7. **Business:** the delivery SLA monitor and daily KPIs (a secondary page, not the focus).
8. **Executor and workers (new):** queued, running, and open executor slots (Airflow StatsD executor metrics), tasks per worker and per queue over time (from `audit.task_runs`), retry counts, worker restarts. Resolve metric names from the installed Airflow 3.3.2 metrics output during Phase 10 and assert the dashboard queries in automated integration tests.

**Alerting (Grafana alerting, as code)**
- Critical: quarantined batch, reconciliation mismatch unrepaired, `BLOCKED` state, backup failure, DAG failure, **no healthy worker for a queue**.
- Warning: freshness near SLA, volume anomaly, new schema column, rising reject rate, **tasks queued longer than N minutes, executor slots exhausted**.
- Route to Mailpit locally; leave the contact point configurable (Slack/email webhook) for deployment.
- Every alert links to a **runbook** in `docs/runbooks/`.

**Deliverables:** provisioned dashboards, alert rules, runbooks.
**Acceptance:** each injected defect class visibly changes a dashboard panel and fires the matching alert; dashboards load from a clean checkout with no manual clicking.

---

### Phase 11: Security, backup, and disaster recovery

**Security controls**

| Area | Control |
|---|---|
| Secrets | No secrets in git (gitleaks in pre-commit and CI); `.env` locally (random hex values), **Docker secrets or an external secret store when deployed**; rotate via a documented procedure. **Known local limitation: database role passwords are visible in `docker inspect` of the Postgres containers until moved to secrets.** |
| Network | **`data-net` is internal (no internet route); the broker and databases are never published**; only Grafana, API, Airflow UI, and the object-store ports are published, on localhost (or behind a reverse proxy with TLS if deployed); **Flower only via a profile, localhost, basic auth** |
| Access | Least-privilege Postgres roles and object-store service accounts; Airflow RBAC; separate credentials per component; **per-queue credentials: only `batch` workers receive the backup role, general workers get ingest-only access**; **workers cannot read the admin-password file** |
| Transport and storage | TLS for Postgres, **the broker**, and the object store when deployed beyond localhost; disk encryption at the host level; server-side encryption optional |
| Containers | Non-root users, pinned image digests, minimal base images, dropped capabilities (**test Postgres, which needs a few capabilities back**), read-only root FS where feasible, **no Docker socket mounted into any container** |
| Supply chain | `pip-audit`, `trivy` image scans, Dependabot/Renovate, lockfiles, **Airflow constraint files; the object-store fork pinned by digest and reviewed as a single-maintainer dependency** |
| Data | Olist is anonymized, but treat zip prefixes and timestamps as quasi-identifiers: expose only what the API allowlist needs |
| Audit | `audit.api_log`, `audit.task_runs`, Airflow task logs, Postgres connection logging for privileged roles |
| Threat model | One page in `docs/`: assets, entry points, threats (SQL injection, credential leak, over-broad role, poisoned input file, **broker access**, **compromised worker**), and the control for each |

**Reliability, backup, and DR**
1. Nightly `pg_dump` of the warehouse and `audit` into the `backups` bucket (compressed, retention 7 daily + 4 weekly), **plus the Airflow metadata database (run history; optional)**; `mc mirror` or versioned object retention for `landing/`.
2. **A restore drill is part of the plan:** a script and runbook rebuild the warehouse from scratch using **landing + dbt** (replay), and separately from a `pg_dump`. Record recovery time (RTO) and the maximum data loss window (RPO), measured.
3. Reliability foundations already exercised include idempotent batch loading, watermark overlap, retries, `max_active_runs=1`, healthchecks, and Valkey AOF with `noeviction`. **Remaining to verify separately:** replay DAG, complete quarantine/alert handling, late acknowledgements, and an explicitly configured visibility timeout exceeding maximum task duration.
4. **The broker holds no state worth backing up**: queued tasks are re-derived by the scheduler. Do not include `valkey-data` in backups.
5. Failure-mode table in docs: for each component (**including each worker, the broker, and the scheduler**), "what happens if it dies, how it is detected, how it recovers".

**Deliverables:** hardening changes, threat model, backup and restore scripts, DR runbook with measured RTO/RPO.
**Acceptance:** a full rebuild from landing and from a dump both succeed and reconcile to the source; scans show no critical vulnerabilities, or each has a documented exception.

---

### Phase 12: CI/CD, documentation, runbooks, demo

**CI (GitHub Actions)**
1. `lint`: ruff, sqlfluff, yamllint, hadolint.
2. `unit`: Python tests, dbt unit tests, DAG import test (`airflow dags list-import-errors`).
3. `dbt`: `dbt parse`/`compile`, `dbt build` on a small seeded sample.
4. `contract`: OpenAPI generation, `oasdiff` breaking-change check, Schemathesis against a CI-started API.
5. `integration`: **current foundation:** `infra/compose.test.yaml` provides disposable PostgreSQL-backed tests, `make test` passes locally, and `.github/workflows/regression.yml` defines the initial regression job. **Future end-to-end CI:** bring up a reduced application stack with one general worker and one batch worker, run the defect detection matrix and `make smoke`, and fail on any miss or false positive. Verify CI runner resources before adding this heavier job.
6. `security`: gitleaks, pip-audit, trivy (planned checks beyond the initial regression workflow).
7. Branch protection: require stable checks that report for every protected PR; documentation-only path exclusions may leave a *required* workflow check pending and must be configured accordingly.

**Current verification:** local Docker-backed `make test` passed. The GitHub-hosted workflow has not yet been reported passing. The controlled one-table DAG failure-isolation exercise is also pending. The current regression workflow ignores commits containing only Markdown changes; mixed code-and-Markdown commits still run it.

**Documentation**
- README: architecture diagram, 5-minute quickstart, screenshots, the detection-rate table, **the single-host caveat for Celery (5.6)**.
- ADRs for every non-obvious choice, runbooks for every alert, the failure-mode table, the threat model, and `dbt docs`/GE Data Docs published to a local static site.

**Maintenance routines (documented and automated where possible)**

| Task | Frequency | Mechanism |
|---|---|---|
| Dependency and image updates | Weekly PRs | Renovate/Dependabot + CI |
| `VACUUM ANALYZE`, partition/retention | Daily | `maintenance` DAG |
| Backup + restore verification | Nightly / monthly drill | `backup` DAG, runbook |
| Reconciliation | Daily | `reconcile` DAG |
| DQ baseline refresh | Weekly | Job updating `audit.metrics_history` baselines |
| Contract review | On change | PR template with a checklist |
| **Object-store supply-chain review** | **Monthly** | **Check the fork's release status and CVE fixes; know the exit plan (5.7)** |

**Retention (cost control)**

| Data | Retention |
|---|---|
| Object store `landing` Parquet | Keep current versions (cheap, replay source); **old overwritten versions expire after 30 days** |
| `raw` tables | Partitioned by load date; drop partitions older than 30-60 days (replayable from landing) |
| `audit` (**including `task_runs`**) | 12 months, then aggregate |
| Airflow logs and metadata | 30 days (`db clean` task); **worker task logs live on the shared log volume** |

**Demo script:** `make demo` starts the stack, runs the emulator at high speed, injects a scripted set of defects, shows each gate catching its target in Grafana, **scales the workers from 2 to 3 and shows the load spreading in Flower and the executor dashboard**, queries the API, and ends with the detection table.

**Acceptance:** a fresh clone reaches a working demo with `cp .env.example .env && make secrets && make demo`; CI is green.

---

## 5. Cross-cutting design decisions

### 5.1 Airflow DAG map

| DAG | Schedule | **Queue** | Role |
|---|---|---|---|
| `ingest` | 15 min | **`ingest`** | Extract, land, Gate 1, load, Gate 2 |
| `transform` | Triggered after successful ingest (asset-triggered) | **`batch`** | `dbt build` (staging, snapshots, marts), Gate 3-4, set `serving_state` |
| `reconcile` | Daily | **`batch`** | Source vs warehouse hashes, repair batches |
| `maintenance` | Daily | **`default`** | Vacuum, partitions, retention, Airflow DB clean |
| `backup` | Nightly | **`batch`** | `pg_dump` to the object store, verify archive |
| `replay_backfill` | Manual (parameters: date range, tables) | **`batch`** | Rebuild raw and marts from landing |
| `dq_report` | Daily | **`default`** | Summarize gate results and detection metrics to `audit` |
| **`infrastructure_smoke_test`** | **Manual** | **`default` and `batch`** | **Proves multi-worker execution and queue isolation** |

### 5.2 Running dbt, Soda, and Great Expectations from Airflow

**Execution choice.** Run dbt in an isolated virtual environment in the Airflow worker image, without mounting the Docker socket into Celery workers.

Instead, **tools whose dependencies conflict with Airflow's constraint file get an isolated virtualenv inside the same Airflow image**, for example `/home/airflow/dbt-venv` with `dbt-postgres` pinned. Tasks call the venv's executable with `BashOperator` (or Astronomer Cosmos in local or virtualenv execution mode) on `queue="batch"`. Apply the same pattern to Soda, and to Great Expectations if it conflicts with the constraints. The benefits: every worker has identical tools, no Docker socket, no cross-container plumbing. The cost: a larger image and memory on the batch worker (hence its cap rises from 2048 to 3072 MiB in Phase 5). Whichever way you invoke dbt, parse `run_results.json` into `audit.dbt_run_results` after every run.

Note: Phase 5 uses the isolated virtualenv inside the Airflow image, not a separate dbt container. Use this design for Phase 5.

### 5.3 Idempotency checklist
- Extraction: deterministic batch ids; manifest written last.
- Load: transactional delete-then-insert by `_batch_id`; unique `batch_registry` key.
- Staging and marts: deterministic `unique_key` merges.
- Landed checkpoint: advances only after durable Parquet + manifest publication and registry transaction. Certified checkpoint: advances only after Gate 2 and never skips an uncertified interval.
- **Celery: retries, worker crashes, and broker redelivery can run any task twice, so every task is written to be safe to repeat.**
- Test: run any interval twice and diff the results; automated in the integration suite.

### 5.4 Division of responsibility between GE, Soda, and dbt tests
Three tools can overlap badly, so each gets a distinct job.

| Tool | Scope | Question it answers |
|---|---|---|
| **Great Expectations** | Batch-level contract on incoming data (Gate 1, optionally on staging rows) | "Is this batch structurally and value-wise acceptable to load?" |
| **dbt tests** | Transformation logic and relational integrity (Gate 3, marts) | "Did our models produce correct, consistent tables?" |
| **Soda** | Table-level SLAs after publish (Gate 4) | "Is the data fresh, complete, stable in volume and distribution, and reconciled?" |

Capability caveat: some Soda features (anomaly detection, change-over-time history, cross-data-source reconciliation) may require a paid/cloud tier depending on the current licensing. This plan avoids depending on them: baselines come from `audit.metrics_history`, and cross-database reconciliation is done by the Airflow reconcile job, with Soda asserting on its result table. Use only locally executable open-source GE validations and Soda SQL checks; paid anomaly detection and cross-source Soda features are explicitly excluded. Validate the pinned package APIs in the Phase 4 test suite.

### 5.5 Cost-effectiveness
- All components are open source and run locally: **zero license cost**.
- One Postgres server hosts warehouse, Airflow metadata, and Celery results. **CeleryExecutor was chosen deliberately to demonstrate multi-instance processing; it costs about 5312 MiB more in container caps than LocalExecutor (5568 MiB with Flower), plus a broker to operate.** Kubernetes is not used.
- Resource limits on every container; **target a laptop or workstation that can give Docker 16 GB and at least 6 CPUs, or a single cloud VM with at least 16 GB RAM (more is comfortable)**; no managed services required.
- The object store stays S3-compatible, so moving to cloud object storage later is a configuration change, not a rewrite.
- Storage is controlled by the retention table in Phase 12.

### 5.6 Executor, queues, and the single-host caveat (new)

| Queue | Worker pool | Slots | Used by |
|---|---|---|---|
| `default` | `airflow-worker-general-1` + `airflow-worker-general-2` | 4 each | General tasks, maintenance, reports |
| `ingest` | `airflow-worker-general-1` + `airflow-worker-general-2` | (shared with `default`) | Per-table extract, validate, and load |
| `batch` | `airflow-worker-batch` ×1 | 2 | dbt, Soda, reconciliation, backup, replay |

- Worker configuration targets 2 × 4 general slots + 2 batch slots = 10 worker slots; scheduler `parallelism` must be read from the **deployed** configuration rather than assumed to be 12 (the recent scheduler reported a DAG concurrency allowance of 16). A task with no `queue` goes to `default`. Tasks sent to a queue with no listening worker can remain queued indefinitely.
- Why separate pools: a long backfill or reconciliation on `batch` cannot starve the 15-minute ingest.
- **What this shows and what it does not:** all workers run on **one host**, so this demonstrates a distributed design, not hardware fault tolerance. Running workers on other machines would additionally need DAG delivery to every host (git-sync or DAGs baked into the image) and remote task logging to the object store (the shared log volume used here only works on a single host). State this in the README.
- **Broker choice:** Valkey (BSD licence, small footprint). RabbitMQ remains an alternative for different delivery semantics. For the deployed Redis-compatible Celery transport, the observed scheduler warning reported an **unconfigured `visibility_timeout`, defaulting to 86,400 seconds (24 hours)**. The earlier assertion that it is already set to 6 hours was obsolete; set and verify an explicit value appropriate to maximum task runtime as part of resilience hardening.
- Broker durability: AOF persistence plus `noeviction`, so a restart does not silently lose queued tasks and a full broker refuses new tasks instead of dropping old ones. It is still a single point of failure.

### 5.7 Object-store decision (new)

I originally specified MinIO without checking its status. Findings (8 Nov 2026): pre-built binaries and Docker images stopped around October 2025; the admin console was stripped from the community edition in May 2025; the project entered maintenance mode on 3 December 2025 and the repository was later archived.

| Option | Verdict |
|---|---|
| **Community fork of MinIO** (the `pgsty` project, renamed from `pgsty/minio` to `pgsty/silo` in August 2026) | **Chosen:** drop-in compatible (same `mc` commands, policies, service accounts). Risks: largely one maintainer, AGPL, image pinned in Compose as `pgsty/silo:RELEASE.2026-10-26T00-00-00Z` |
| RustFS (Apache-2.0) | Candidate replacement; bucket versioning and scoped IAM policies unverified |
| SeaweedFS | Mature; more moving parts |
| Garage | One comparison says it lacks bucket policies and versioning, which this design needs |
| Build MinIO from source | Official code; you own patching and signing |

The service stays named `minio` and the variables stay `MINIO_*`, with the image in `S3_IMAGE`, so a swap is a configuration change. Pin by digest, review monthly, and keep the exit plan in an ADR.

---

## 6. Defect catalog (spec for gates and tests)

Action codes: **Q-B** quarantine batch · **R-R** reject rows to `quarantine.rejected_rows` · **BLK** block publish/serving · **WARN** alert only · **FIX** auto-repair · **RETRY** automatic retry ends in the clean-run state.

| ID | Defect | Injected by | Detected by (gate) | Action |
|---|---|---|---|---|
| D01 | Duplicate rows within a batch | Emulator duplicate-key bug | GE key uniqueness (G1) + dbt `unique` (G3) | R-R / dedupe |
| D02 | Duplicates across batches (overlap re-extract) | Watermark overlap (by design) | Staging dedupe; dbt `unique` (G3) | Dedupe |
| D03 | Corrupt or truncated Parquet | Chaos: truncate file | Readability + checksum (G1) | Q-B |
| D04 | Manifest/row-count mismatch | Chaos: edit count or drop rows | Manifest check (G1), load count (G2) | Q-B |
| D05 | Schema drift: added column | Chaos: add column in source | GE schema contract (G1) | WARN, hold column |
| D06 | Schema drift: renamed/removed/type-changed column | Chaos: DDL on source | GE schema contract (G1) | Q-B |
| D07 | Nulls in required fields | Emulator bad values | GE not-null (G1) | Q-B or R-R by field |
| D08 | Out-of-range or impossible values (negative price, score outside 1-5, future timestamps, wrong timestamp order) | Emulator bad values | GE ranges (G1); dbt singular tests (G3) | R-R |
| D09 | Invalid categoricals (unknown status, invalid state code) | Emulator bad values | GE value sets (G1); dbt `accepted_values` (G3) | R-R / Q-B for status |
| D10 | Orphan foreign keys (item before product/seller) | Emulator lazy master data | dbt `relationships` with severity (G3) | `unknown` member, WARN |
| D11 | Late or out-of-order updates | Chaos: delay batches, swap order | Staging latest-wins; end-state equals source (G4 reconciliation) | Correct by design |
| D12 | Silent updates (`updated_at` not bumped) | Emulator trigger bypass | Row-hash reconciliation | FIX + alert |
| D13 | Hard deletes in source | Emulator deletes | Key reconciliation | FIX (tombstone) + alert |
| D14 | Source vs warehouse count drift | Chaos: drop rows in raw | Reconciliation + Soda on result table | BLK + FIX |
| D15 | Missing or stale batch (extract did not run) | Chaos: pause DAG | Soda freshness; watermark-lag alert; zero-row heartbeat | WARN, BLK if past SLA |
| D16 | Volume anomaly (batch far smaller or larger than baseline) | Chaos: scale a batch | Soda volume check vs baseline | WARN or BLK by size |
| D17 | Distribution drift (null rate, mean payment, `unknown`-member share) | Chaos: skew a column | Soda distribution checks | WARN |
| D18 | SCD2 overlap or multiple current rows | Chaos: corrupt snapshot | dbt singular tests (G3) | BLK |
| D19 | Text noise (whitespace, case variants) | Emulator noise | Normalization in staging; dbt `accepted_values` post-normalization | Normalize |
| D20 | PII or non-allowlisted field in an API response | Chaos: add column to `api` view | dbt contract + Schemathesis + response-model test | CI fail |
| D21 | Aggregate inconsistency (items + freight vs payments) | Chaos: alter payments | dbt singular test with calibrated tolerance; Soda cross-table | WARN or BLK |
| D22 | Replay of an already-loaded batch | Chaos: rerun DAG run | Unique `batch_registry` key; idempotent load | No-op, logged |
| **D23** | **Worker killed mid-task (infrastructure)** | **Chaos: `docker kill` a worker during a run** | **Task heartbeat timeout marks the task failed; `audit.task_runs` shows the retry on another worker; final state equals clean run** | **RETRY** |
| **D24** | **Broker restarted with tasks queued (infrastructure)** | **Chaos: restart `valkey` during a run** | **AOF restores the queue; stuck-queued detection and scheduler requeue; no task lost; alert if queued beyond N minutes** | **RETRY + WARN** |
| **D25** | **Task executed twice by redelivery (infrastructure)** | **Chaos: shrink the visibility timeout below a task's runtime** | **Unique `batch_registry` key and idempotent load make the second run a no-op (builds on D22); duplicate execution visible in `audit.task_runs`** | **No-op, logged** |

New defects found by the fuzz job (Phase 8) are appended here with their ID, check, and test.

---

## 7. Definition of done (whole project)

- [ ] `make demo` works from a fresh clone.
- [ ] Defect matrix: **100% detection** of cataloged defects (**D01-D25**), **0 false positives** on clean runs, run in CI.
- [ ] Source reconciliation shows zero unexplained differences after a full run, and after repair of injected silent updates and deletes.
- [ ] Idempotency test passes: every interval and batch replays to identical state, **including after a worker kill, a broker restart, and a forced redelivery**.
- [ ] **`make smoke` passes: tasks demonstrably ran on at least 2 general workers, and `audit.task_runs` confirms it for the ingest DAG.**
- [ ] Full rebuild from landing and from a dump both verified, with RTO and RPO recorded.
- [ ] API contract committed, versioned, CI-enforced; Schemathesis green; breaking-change gate active.
- [ ] Grafana dashboards and alerts provisioned as code; every alert has a runbook.
- [ ] Security checklist complete; no critical scan findings unexplained; no secrets in git history; **no Docker socket mounted anywhere**.
- [ ] Total memory use stays under the stated budget (**worst-case caps 12736 MiB**; Docker allocation 16 GB).
- [ ] **ADRs exist for the object-store fork, CeleryExecutor and its single-host limits, and the broker choice.**
- [ ] README includes architecture diagram, detection table, failure-mode table, and ADR links.

---

## 8. Risk register

| Risk | Impact | Mitigation |
|---|---|---|
| **Laptop resource exhaustion** | **Slow or crashing stack** | **Resource limits, compose profiles (`--profile core/obs/api/monitoring`), reduce emulator speed, scale workers down (`make scale-workers N=1`), Docker allocation 16 GB** |
| Tool capability or licensing changes (Soda, GE) | Rework of Gate 4 | Keep Soda checks simple; baselines and reconciliation in own code; verify early (5.4) |
| Scope creep | Project never finishes | Build in phase order; the API and Grafana come after the gates are proven |
| Olist natural inconsistencies produce false alarms | Noisy gates | Profile clean data first; calibrate thresholds from baselines; severity levels |
| Over-engineered incremental logic | Hard-to-debug models | Start with `unique_key` + lookback; add complexity only when a test demands it |
| Airflow version differences (2.x vs 3.x) | Broken DAG patterns | Pin the version; use the matching asset/dataset API; DAG import test in CI |
| Reconciliation cost grows with data | Slow daily job | Bucketed hashes, drill down only on mismatch; fine at Olist scale |
| **Object-store supply chain (upstream archived, fork maintained by one person)** | **Unpatched vulnerabilities, or the fork disappears** | **Pin by digest, monthly review, S3-generic code, exit plan to RustFS or SeaweedFS in an ADR (5.7)** |
| **Broker is a single point of failure** | **Pipeline stalls if Valkey is down** | **AOF persistence, healthcheck and alert, restart policy, D24 drill; document that it is not highly available** |
| **Task runs twice (redelivery, retry)** | **Duplicate loads or side effects** | **Idempotent tasks everywhere, verify explicit broker visibility timeout (observed default 24 h), run D25 drill** |
| **Worker out of memory** | **Task killed (exit 137), retried or failed** | **Per-worker caps, low concurrency, memory-bounded validation (Phase 4), alert on worker restarts** |
| **Connection exhaustion with parallel tasks** | **`too many clients` or failed loads** | **Role connection limits, Airflow pools below those limits, `max_connections=150`** |
| **Airflow 3.3 option names differ from this plan** | **Login or Celery misconfiguration** | **Validate the actual Compose environment and Airflow configuration against the installed version** |

---

## 9. Next implementation priorities

The initial infrastructure, emulator, and seven-table extraction/certification pipeline are implemented and have been exercised. Do **not** restart the project from Phase 0 or the one-table proof of concept.

1. **Verify remote CI** for `.github/workflows/regression.yml`; confirm that the local `make test` suite runs with the same test isolation in GitHub Actions.
2. **Finish DAG failure isolation testing:** inject a controlled failure into one mapped table in an isolated environment and prove the other table chains still certify successfully; preserve the production source.
3. **Advance Phases 5–6:** complete and validate dbt staging, rejection handling, marts and the required contract tests. The presence of dbt files is not evidence of accepted models.
4. **Extend Phase 7:** integrate dbt/Soda gates and reconcile against downstream state, then add explicit repair/tombstone processing; retain the current continuous/full raw-state reconciliation as the tested foundation.
5. **Complete Phases 8–12:** controlled defect matrix, serving API, observability, security/backup drills, final release CI and demonstration. Update the status table only with verified acceptance evidence.

Use the repository's current README as the **project-facing progress summary**. This file is a detailed implementation roadmap and should **not** be added to the README as a mandatory reference or dependency.
