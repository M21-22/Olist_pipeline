-- Database access
REVOKE ALL ON DATABASE warehouse FROM public;

GRANT CONNECT ON DATABASE warehouse TO
etl_loader,
dbt_runner,
soda_reader,
api_reader,
grafana_reader;

-- Restrict public schema.
REVOKE CREATE ON SCHEMA public FROM public;

-- ETL loader: raw, audit, quarantine.
GRANT USAGE, CREATE ON SCHEMA raw, audit, quarantine
TO etl_loader;

-- dbt: read raw and manage transformations.
GRANT USAGE ON SCHEMA raw TO dbt_runner;

GRANT USAGE, CREATE ON SCHEMA staging, marts, api
TO dbt_runner;

-- Data quality: read warehouse data.
GRANT USAGE ON SCHEMA raw, staging, marts, audit
TO soda_reader;

-- API: only approved serving views.
GRANT USAGE ON SCHEMA api TO api_reader;

-- Monitoring: audit and selected marts.
GRANT USAGE ON SCHEMA audit, marts TO grafana_reader;

-- Existing raw tables.
GRANT SELECT ON ALL TABLES IN SCHEMA raw
TO dbt_runner, soda_reader;

-- Existing analytical tables.
GRANT SELECT ON ALL TABLES IN SCHEMA staging, marts
TO soda_reader;

-- Existing API views.
GRANT SELECT ON ALL TABLES IN SCHEMA api
TO api_reader;

-- Existing audit tables.
GRANT SELECT ON ALL TABLES IN SCHEMA audit
TO soda_reader, grafana_reader;

-- Existing marts for monitoring.
GRANT SELECT ON ALL TABLES IN SCHEMA marts
TO grafana_reader;

-- Future raw tables created by etl_loader.
ALTER DEFAULT PRIVILEGES FOR ROLE etl_loader
IN SCHEMA raw
GRANT SELECT ON TABLES TO dbt_runner, soda_reader;

-- Future audit tables created by etl_loader.
ALTER DEFAULT PRIVILEGES FOR ROLE etl_loader
IN SCHEMA audit
GRANT SELECT ON TABLES TO soda_reader, grafana_reader;

-- Future staging and mart tables created by dbt_runner.
ALTER DEFAULT PRIVILEGES FOR ROLE dbt_runner
IN SCHEMA staging, marts
GRANT SELECT ON TABLES TO soda_reader;

-- Future mart tables for Grafana.
ALTER DEFAULT PRIVILEGES FOR ROLE dbt_runner
IN SCHEMA marts
GRANT SELECT ON TABLES TO grafana_reader;

-- Future API views created by dbt_runner.
ALTER DEFAULT PRIVILEGES FOR ROLE dbt_runner
IN SCHEMA api
GRANT SELECT ON TABLES TO api_reader;
