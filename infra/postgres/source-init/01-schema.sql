-- Operational source data.
CREATE SCHEMA IF NOT EXISTS shop AUTHORIZATION emulator_writer;

-- Emulator state and injected-defect ground truth.
-- The extractor must not have access to this schema.
CREATE SCHEMA IF NOT EXISTS chaos AUTHORIZATION emulator_writer;
