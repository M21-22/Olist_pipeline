-- Reporting cursor for the latest successfully processed certification.
-- Applied to fresh databases during initialization and to existing databases as a migration.
CREATE TABLE IF NOT EXISTS audit.dbt_staging_progress (
    model_name TEXT PRIMARY KEY,
    last_certified_at TIMESTAMPTZ,
    last_batch_id TEXT,
    run_cutoff_at TIMESTAMPTZ,
    run_cutoff_batch_id TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT last_cursor_pair CHECK (
        (last_certified_at IS NULL) = (last_batch_id IS NULL)
    ),
    CONSTRAINT run_cursor_pair CHECK (
        (run_cutoff_at IS NULL) = (run_cutoff_batch_id IS NULL)
    )
);

GRANT SELECT, INSERT, UPDATE ON audit.dbt_staging_progress TO dbt_runner;
