-- Raw ingestion
CREATE SCHEMA IF NOT EXISTS raw;

-- Transformation layer
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS snapshots;
CREATE SCHEMA IF NOT EXISTS marts;

-- Data quality and operations
CREATE SCHEMA IF NOT EXISTS audit;
CREATE SCHEMA IF NOT EXISTS quarantine;

-- Serving layer
CREATE SCHEMA IF NOT EXISTS api;
