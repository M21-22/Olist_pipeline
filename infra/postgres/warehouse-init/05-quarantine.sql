-- 05-quarantine.sql | warehouse initialization
-- Rejected business rows and source-delete repairs.
CREATE TABLE quarantine.rejected_rows (
    rejected_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_table TEXT NOT NULL,
    business_key TEXT,
    batch_id TEXT NOT NULL,
    reason_code TEXT NOT NULL,
    raw_row JSONB NOT NULL,
    rejected_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (source_table, business_key, batch_id, reason_code)
);
