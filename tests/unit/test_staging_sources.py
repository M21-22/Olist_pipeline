"""Static checks for staging source filtering and reference snapshots."""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DBT = ROOT / "dbt"
MODELS = DBT / "models" / "staging"
MUTABLE = (
    "customers", "orders", "order_items", "order_payments",
    "order_reviews", "products", "sellers",
)
STATIC = ("geolocation", "product_category_translation")


def test_staging_defaults_to_incremental():
    config = (DBT / "dbt_project.yml").read_text()
    block = config.split("    staging:", 1)[1].split("    marts:", 1)[0]
    assert "+materialized: incremental" in block
    assert "+incremental_strategy: merge" in block


def test_mutable_models_filter_only_certified_and_apply_shared_key_encoding():
    for table in MUTABLE:
        sql = (MODELS / f"stg_{table}.sql").read_text()
        assert "registry.status = 'CERTIFIED'" in sql, table
        assert "registry.table_name = '" + table + "'" in sql, table
        assert "ROW_NUMBER() OVER" in sql, table
        assert "src.updated_at DESC" in sql, table
        assert "registry.certified_at DESC" in sql, table
        assert "encode_business_key(" in sql, table
        assert "deleted.source_table = '" + table + "'" in sql, table


def test_snapshot_models_use_latest_certified_snapshot():
    for table in STATIC:
        sql = (MODELS / f"stg_{table}.sql").read_text()
        assert "registry.status = 'CERTIFIED'" in sql, table
        assert "landed.extraction_mode = 'snapshot'" in sql, table
        assert "latest_snapshot" in sql, table
