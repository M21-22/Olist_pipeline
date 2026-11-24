-- Requires pre-created login roles and warehouse schemas.
-- dbt manages SELECT grants on its own model objects.
REVOKE ALL ON DATABASE :"DBNAME" FROM public;
REVOKE CREATE ON SCHEMA public FROM public;

GRANT CONNECT ON DATABASE :"DBNAME" TO
etl_loader, ingest_audit_writer, dbt_runner, soda_reader,
api_reader, grafana_reader;

GRANT USAGE ON SCHEMA raw TO etl_loader, dbt_runner;
GRANT USAGE ON SCHEMA audit TO
etl_loader, ingest_audit_writer, dbt_runner, soda_reader,
grafana_reader, api_reader;
GRANT USAGE ON SCHEMA quarantine TO dbt_runner, grafana_reader;
GRANT USAGE ON SCHEMA staging TO dbt_runner;
GRANT USAGE ON SCHEMA marts TO dbt_runner, soda_reader, grafana_reader;
GRANT USAGE ON SCHEMA api TO dbt_runner, api_reader, grafana_reader;

-- dbt_runner creates its own model tables and views in these schemas.
GRANT CREATE ON SCHEMA staging, marts, api TO dbt_runner;

-- Persistent raw tables are not owned by dbt.
GRANT SELECT, INSERT, DELETE ON ALL TABLES IN SCHEMA raw TO etl_loader;
GRANT SELECT ON ALL TABLES IN SCHEMA raw TO dbt_runner;

-- Also remove legacy grants when this file is reapplied to an existing warehouse.
REVOKE USAGE ON SCHEMA raw, staging FROM soda_reader;
REVOKE SELECT ON ALL TABLES IN SCHEMA raw, staging, audit, marts FROM soda_reader;
DO $$
DECLARE
    model_name text;
BEGIN
    FOREACH model_name IN ARRAY ARRAY[
        'gate4_metrics', 'fct_orders', 'fct_order_items', 'fct_payments', 'mart_daily_kpis'
    ] LOOP
        IF to_regclass(format('marts.%I', model_name)) IS NOT NULL THEN
            EXECUTE format('GRANT SELECT ON marts.%I TO soda_reader', model_name);
        END IF;
    END LOOP;
END
$$;

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

-- Repair writes bypass ordinary ingestion checkpoints.
GRANT INSERT ON audit.landed_batches, audit.reconciliation TO etl_loader;
GRANT SELECT, INSERT, DELETE ON audit.deleted_keys TO etl_loader;

-- Transformations, reconciliation and serving status
GRANT SELECT, INSERT, UPDATE ON
audit.reconciliation,
audit.metrics_history,
audit.serving_state,
audit.dbt_staging_progress
TO dbt_runner;

GRANT SELECT ON
audit.batch_registry,
audit.landed_batches,
audit.certified_watermarks
TO dbt_runner;

GRANT SELECT, INSERT ON audit.dbt_staging_processed_batches TO dbt_runner;
GRANT INSERT ON audit.dq_results TO dbt_runner;
GRANT SELECT (batch_id, gate, check_name, table_name, status, failed_count, details, checked_at),
      UPDATE (status, failed_count, details, checked_at)
ON audit.dq_results TO dbt_runner;

-- dbt-postgres uses temporary relations during incremental merges.
GRANT TEMPORARY ON DATABASE :"DBNAME" TO dbt_runner;

GRANT SELECT ON
audit.extraction_watermarks,
audit.landed_batches,
audit.certified_watermarks,
audit.batch_registry,
audit.task_runs,
audit.dq_results,
audit.reconciliation,
audit.metrics_history,
audit.serving_state,
audit.deleted_keys
TO grafana_reader;

GRANT SELECT ON audit.reconciliation, audit.metrics_history, audit.soda_drift_calibration
TO soda_reader;
GRANT SELECT ON audit.serving_state TO api_reader;

-- Rejected business rows
GRANT SELECT, INSERT, UPDATE, DELETE ON quarantine.rejected_rows TO dbt_runner;
GRANT SELECT ON quarantine.rejected_rows TO grafana_reader;

-- Confirmed source deletions
GRANT SELECT, INSERT, UPDATE, DELETE ON audit.deleted_keys TO dbt_runner;

-- Identity sequences
GRANT USAGE ON ALL SEQUENCES IN SCHEMA audit TO etl_loader, dbt_runner;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA quarantine TO dbt_runner;
