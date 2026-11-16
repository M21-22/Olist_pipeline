#!/usr/bin/env bash
set -euo pipefail
# This runs only in the disposable PostgreSQL test container.
psql -v ON_ERROR_STOP=1 --username postgres --dbname postgres <<'SQL'
CREATE ROLE etl_loader LOGIN PASSWORD 'ci_loader_only';
CREATE ROLE olist_test_admin LOGIN PASSWORD 'ci_admin_only';
CREATE DATABASE warehouse_gate2_test;
SQL
for file in 02-schemas.sql 03-raw.sql 04-audit.sql 05-quarantine.sql; do
  psql -v ON_ERROR_STOP=1 --username postgres --dbname warehouse_gate2_test -f "/olist-ddl/$file"
done
psql -v ON_ERROR_STOP=1 --username postgres --dbname warehouse_gate2_test <<'SQL'
GRANT CONNECT ON DATABASE warehouse_gate2_test TO etl_loader, olist_test_admin;
GRANT USAGE ON SCHEMA raw, audit TO etl_loader;
GRANT SELECT, INSERT, DELETE ON ALL TABLES IN SCHEMA raw TO etl_loader;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA audit TO etl_loader;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA audit TO etl_loader;
GRANT USAGE ON SCHEMA raw, audit, quarantine TO olist_test_admin;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA raw, audit, quarantine TO olist_test_admin;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA audit, quarantine TO olist_test_admin;
SQL
