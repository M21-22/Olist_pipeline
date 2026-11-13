-- Run after the warehouse's persistent infrastructure tables are created.
-- All login roles must already exist via scripts/create-db-roles.sh.
-- No default privileges: dbt config manages grants on its own models.
REVOKE ALL ON DATABASE warehouse FROM public;
REVOKE CREATE ON SCHEMA public FROM public;

GRANT CONNECT ON DATABASE warehouse TO
etl_loader, ingest_audit_writer, dbt_runner, soda_reader,
api_reader, grafana_reader;

GRANT USAGE ON SCHEMA raw TO etl_loader, dbt_runner, soda_reader;
GRANT USAGE ON SCHEMA audit TO
etl_loader, ingest_audit_writer, dbt_runner, soda_reader,
grafana_reader, api_reader;
GRANT USAGE ON SCHEMA quarantine TO dbt_runner, grafana_reader;
GRANT USAGE ON SCHEMA staging TO dbt_runner, soda_reader;
GRANT USAGE ON SCHEMA snapshots TO dbt_runner, soda_reader;
GRANT USAGE ON SCHEMA marts TO dbt_runner, soda_reader, grafana_reader;
GRANT USAGE ON SCHEMA api TO dbt_runner, api_reader, grafana_reader;

-- dbt_runner creates its own model tables and views in these schemas.
GRANT CREATE ON SCHEMA staging, snapshots, marts, api TO dbt_runner;

-- Persistent raw tables are not owned by dbt.
GRANT SELECT, INSERT, DELETE ON ALL TABLES IN SCHEMA raw TO etl_loader;
GRANT SELECT ON ALL TABLES IN SCHEMA raw TO dbt_runner, soda_reader;

-- Extraction audit
GRANT SELECT, INSERT, UPDATE ON
audit.extraction_watermarks,
audit.landed_batches,
audit.task_runs
TO ingest_audit_writer;

GRANT SELECT ON
audit.extraction_watermarks,
audit.landed_batches,
audit.task_runs
TO etl_loader;

-- Batch certification
GRANT SELECT, INSERT, UPDATE ON
audit.certified_watermarks,
audit.batch_registry,
audit.dq_results
TO etl_loader;

-- Transformations, reconciliation and serving status
GRANT SELECT, INSERT, UPDATE ON
audit.reconciliation,
audit.metrics_history,
audit.serving_state
TO dbt_runner;

GRANT SELECT ON
audit.batch_registry,
audit.serving_state
TO dbt_runner;

GRANT SELECT ON ALL TABLES IN SCHEMA audit TO soda_reader, grafana_reader;
GRANT SELECT ON audit.serving_state TO api_reader;

-- Rejected business rows
GRANT SELECT, INSERT, UPDATE, DELETE ON quarantine.rejected_rows TO dbt_runner;
GRANT SELECT ON quarantine.rejected_rows TO grafana_reader;

-- Confirmed source deletions
GRANT SELECT, INSERT, UPDATE, DELETE ON audit.deleted_keys TO dbt_runner;

-- Identity sequences
GRANT USAGE ON ALL SEQUENCES IN SCHEMA audit TO etl_loader, dbt_runner;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA quarantine TO dbt_runner;
