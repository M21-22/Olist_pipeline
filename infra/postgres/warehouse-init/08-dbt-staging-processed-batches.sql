-- Durable per-model batch membership; certification timestamps can arrive out of order.
-- Membership changes commit in the same transaction as the staging merge.
CREATE TABLE IF NOT EXISTS audit.dbt_staging_processed_batches (
    model_name TEXT NOT NULL,
    batch_id TEXT NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (model_name, batch_id)
);
GRANT SELECT, INSERT ON audit.dbt_staging_processed_batches TO dbt_runner;
