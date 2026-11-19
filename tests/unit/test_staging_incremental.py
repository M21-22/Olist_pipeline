"""Static checks for incremental staging configuration and hooks."""
from pathlib import Path
import re

DBT = Path(__file__).resolve().parents[2] / "dbt"
MUTABLE = {
    "customers": "customer_id",
    "orders": "order_id",
    "order_items": "item_pk",
    "order_payments": "order_id, payment_sequential",
    "order_reviews": "review_pk",
    "products": "product_id",
    "sellers": "seller_id",
}


def test_staging_incremental_contract():
    project = (DBT / "dbt_project.yml").read_text()
    assert "+materialized: incremental" in project
    assert "+incremental_strategy: merge" in project
    for table in MUTABLE:
        sql = (DBT / "models/staging" / f"stg_{table}.sql").read_text()
        assert f"staging_prepare('{table}')" in sql
        assert f"staging_delete_tombstoned('{table}'" in sql
        assert "staging_finalize()" in sql
        assert f"staging_incremental_key_filter('{table}'" in sql
        assert "ROW_NUMBER() OVER" in sql
        assert "registry.status = 'CERTIFIED'" in sql
        assert "config(materialized='table')" not in sql


def test_durable_cursor_hooks_and_deletion():
    macro = (DBT / "macros/staging_incremental.sql").read_text()
    assert "ON CONFLICT (model_name) DO UPDATE" in macro
    assert "last_certified_at = run_cutoff_at" in macro
    # A timestamp cursor alone can lose batches certified out of order.
    assert "pg_temp.olist_staging_pending_batches" in macro
    assert "dbt_staging_processed_batches" in macro
    assert "DELETE FROM {{ this }} AS target" in macro
    assert "is_incremental()" in macro


def test_reference_models_remain_tables():
    for table in ("geolocation", "product_category_translation"):
        sql = (DBT / "models/staging" / f"stg_{table}.sql").read_text()
        assert "materialized='table'" in sql
