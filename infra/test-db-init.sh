#!/usr/bin/env bash
set -euo pipefail
# Disposable PostgreSQL test database only. Not for production use.
psql -v ON_ERROR_STOP=1 --username postgres --dbname postgres <<'SQL'
CREATE ROLE etl_loader LOGIN PASSWORD 'ci_loader_only';
CREATE ROLE olist_test_admin LOGIN PASSWORD 'ci_admin_only';
CREATE ROLE dbt_runner LOGIN PASSWORD 'ci_dbt_only';
CREATE ROLE soda_reader NOLOGIN;
CREATE ROLE ingest_audit_writer NOLOGIN;
CREATE ROLE grafana_reader NOLOGIN;
CREATE ROLE api_reader NOLOGIN;
CREATE DATABASE warehouse_test;
SQL

# Use the same warehouse SQL as bootstrap. The Airflow metadata database is not needed.
for file in /olist-ddl/*.sql; do
  case "${file##*/}" in
    01-*) continue ;;
  esac
  psql -v ON_ERROR_STOP=1 --username postgres --dbname warehouse_test -f "$file"
done

psql -v ON_ERROR_STOP=1 --username postgres --dbname warehouse_test <<'SQL'
CREATE SCHEMA shop;
CREATE TABLE shop.customers (
    customer_id TEXT PRIMARY KEY,
    customer_unique_id TEXT,
    customer_zip_code_prefix TEXT,
    customer_city TEXT,
    customer_state TEXT,
    updated_at TIMESTAMPTZ NOT NULL
);

GRANT CONNECT ON DATABASE warehouse_test TO olist_test_admin;
GRANT SELECT ON audit.dbt_staging_progress TO olist_test_admin;
GRANT SELECT ON audit.dbt_staging_processed_batches TO olist_test_admin;
GRANT SELECT, INSERT, DELETE ON audit.deleted_keys TO olist_test_admin;
GRANT USAGE ON SCHEMA quarantine TO olist_test_admin;
GRANT SELECT ON quarantine.rejected_rows TO olist_test_admin;

-- Loader permissions retained from the existing test contract.
GRANT USAGE ON SCHEMA raw, audit TO etl_loader;
GRANT SELECT, INSERT, DELETE ON ALL TABLES IN SCHEMA raw TO etl_loader;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA audit TO etl_loader;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA audit TO etl_loader;

-- Fixture administrator: grant only what the integration tests need.
GRANT USAGE ON SCHEMA shop, raw, audit TO olist_test_admin;
GRANT SELECT, INSERT, UPDATE, DELETE ON shop.customers TO olist_test_admin;
GRANT SELECT, INSERT, DELETE ON raw.customers, raw.orders, raw.order_items,
    raw.order_payments, raw.order_reviews, raw.products, raw.sellers TO olist_test_admin;
GRANT SELECT, INSERT, DELETE ON audit.landed_batches TO olist_test_admin;
GRANT SELECT, INSERT, DELETE ON audit.certified_watermarks TO olist_test_admin;

-- Gate 1 and Gate 2 use INSERT ... ON CONFLICT DO UPDATE.
GRANT SELECT, INSERT, UPDATE, DELETE ON audit.batch_registry TO olist_test_admin;
GRANT SELECT, INSERT, UPDATE, DELETE ON audit.dq_results TO olist_test_admin;

-- Reconciliation inserts results; tests read and clean them up.
GRANT SELECT, INSERT, DELETE ON audit.reconciliation TO olist_test_admin;
SQL
