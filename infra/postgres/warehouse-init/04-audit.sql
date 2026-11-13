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
    certified_at TIMESTAMPTZ
);

CREATE INDEX batch_registry_table_status_idx ON audit.batch_registry (table_name, status);

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
    checked_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
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
    PRIMARY KEY (metric_name, object_name, measured_at)
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
