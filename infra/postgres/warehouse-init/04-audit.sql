-- 04-audit.sql | warehouse initialization
-- Extraction, certification, DQ, monitoring, and reconciliation state.
CREATE TABLE audit.extraction_watermarks (
    table_name TEXT PRIMARY KEY,
    landed_high_watermark TIMESTAMPTZ,
    last_landed_batch_id TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE audit.landed_batches (
    batch_id TEXT PRIMARY KEY,
    table_name TEXT NOT NULL,
    extraction_mode TEXT NOT NULL CHECK (extraction_mode IN ('incremental', 'snapshot', 'repair')),
    logical_interval_start TIMESTAMPTZ,
    logical_interval_end TIMESTAMPTZ NOT NULL,
    parquet_key TEXT NOT NULL,
    manifest_key TEXT NOT NULL,
    row_count BIGINT NOT NULL CHECK (row_count >= 0),
    published_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (table_name, logical_interval_end, extraction_mode)
);

CREATE TABLE audit.certified_watermarks (
    table_name TEXT PRIMARY KEY,
    certified_high_watermark TIMESTAMPTZ,
    last_certified_batch_id TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- GATE 1 status and GATE 2 certification lifecycle; one row per batch.
CREATE TABLE audit.batch_registry (
    batch_id TEXT PRIMARY KEY REFERENCES audit.landed_batches (batch_id),
    table_name TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('LANDED', 'VALIDATING', 'QUARANTINED', 'LOADING', 'CERTIFIED', 'FAILED')),
    expected_rows BIGINT NOT NULL CHECK (expected_rows >= 0),
    actual_rows BIGINT,
    expected_control_totals JSONB NOT NULL DEFAULT '{}'::JSONB,
    actual_control_totals JSONB,
    reason TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    certified_at TIMESTAMPTZ,
    -- NULL identifies an ordinary ingestion batch.
    repair_observed_at TIMESTAMPTZ
);

CREATE INDEX batch_registry_table_status_idx ON audit.batch_registry (table_name, status);

-- Reporting cursor for the latest successfully processed certification.
CREATE TABLE audit.dbt_staging_progress (
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

-- Durable per-model batch membership; certification timestamps can arrive out of order.
-- Membership changes commit in the same transaction as the staging merge.
CREATE TABLE audit.dbt_staging_processed_batches (
    model_name TEXT NOT NULL,
    batch_id TEXT NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (model_name, batch_id)
);

CREATE TABLE audit.task_runs (
    dag_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    task_id TEXT NOT NULL,
    map_index INTEGER NOT NULL DEFAULT -1,
    try_number INTEGER NOT NULL,
    batch_id TEXT,
    worker_hostname TEXT,
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ,
    status TEXT NOT NULL,
    error_summary TEXT,
    PRIMARY KEY (dag_id, run_id, task_id, map_index, try_number)
);

-- GATE 1 / later gates: persists validation outcomes for inspection.
CREATE TABLE audit.dq_results (
    result_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    batch_id TEXT,
    table_name TEXT NOT NULL,
    gate TEXT NOT NULL,
    check_name TEXT NOT NULL,
    status TEXT NOT NULL,
    failed_count BIGINT NOT NULL DEFAULT 0,
    details JSONB NOT NULL DEFAULT '{}'::JSONB,
    checked_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT BGC UNIQUE (batch_id, gate, check_name)
);

CREATE TABLE audit.reconciliation (
    reconciliation_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    table_name TEXT NOT NULL,
    source_row_count BIGINT,
    target_row_count BIGINT,
    mismatch_count BIGINT NOT NULL DEFAULT 0,
    unrepaired_count BIGINT NOT NULL DEFAULT 0,
    status TEXT NOT NULL,
    checked_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    details JSONB NOT NULL DEFAULT '{}'::JSONB
);

CREATE TABLE audit.metrics_history (
    metric_name TEXT NOT NULL,
    object_name TEXT NOT NULL,
    measured_at TIMESTAMPTZ NOT NULL,
    metric_value DOUBLE PRECISION NOT NULL,
    source_interval_end TIMESTAMPTZ,
    baseline_eligible BOOLEAN NOT NULL DEFAULT TRUE,
    PRIMARY KEY (metric_name, object_name, measured_at)
);

CREATE INDEX metrics_history_series_window_idx
    ON audit.metrics_history (metric_name, object_name, source_interval_end, measured_at);

CREATE TABLE audit.soda_drift_calibration (
    metric_name TEXT NOT NULL,
    object_name TEXT NOT NULL,
    absolute_floor DOUBLE PRECISION NOT NULL CHECK (absolute_floor > 0 AND absolute_floor < 'Infinity'),
    fit_samples INTEGER NOT NULL CHECK (fit_samples >= 14),
    validation_samples INTEGER NOT NULL CHECK (validation_samples >= 7),
    default_validation_warnings INTEGER NOT NULL,
    calibrated_validation_warnings INTEGER NOT NULL,
    first_source_interval_end TIMESTAMPTZ NOT NULL,
    last_source_interval_end TIMESTAMPTZ NOT NULL,
    calibrated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (metric_name, object_name)
);

CREATE TABLE audit.serving_state (
    state_id INTEGER PRIMARY KEY CHECK (state_id = 1),
    status TEXT NOT NULL CHECK (status IN ('HEALTHY', 'DEGRADED', 'BLOCKED')),
    last_successful_transform_at TIMESTAMPTZ,
    reason TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO audit.serving_state (state_id, status) VALUES (1, 'BLOCKED');

CREATE TABLE audit.deleted_keys (
    source_table TEXT NOT NULL,
    business_key TEXT NOT NULL,
    deleted_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (source_table, business_key)
);
