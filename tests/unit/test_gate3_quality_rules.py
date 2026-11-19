"""Static checks for shared current-state quality rules; no database needed."""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
TABLES = (
    "customers", "orders", "order_items", "order_payments", "order_reviews",
    "products", "sellers", "geolocation", "product_category_translation",
)


def test_every_staging_model_has_validity_and_provenance():
    for name in TABLES:
        content = (ROOT / f"dbt/models/staging/stg_{name}.sql").read_text()
        assert f"staging_is_valid('{name}', 'src')" in content, name
        assert "AS is_valid" in content, name
        assert "AS source_batch_id" in content, name


def test_quality_candidates_use_current_staging_not_history():
    model = (ROOT / "dbt/models/quality/rejected_rows_candidates.sql").read_text()
    macro = (ROOT / "dbt/macros/find_rejected_rows.sql").read_text()
    for name in TABLES:
        assert f"find_rejected_rows('{name}'" in model
    assert "ref('stg_' ~ table_name)" in macro
    assert "stg.source_batch_id" in macro
    assert "stg.is_valid = FALSE" in macro
    assert "TO_JSONB(raw_src)" in macro


def test_rejection_sync_is_idempotent():
    macro = (ROOT / "dbt/macros/sync_rejected_rows.sql").read_text()
    assert "ON CONFLICT (source_table, business_key, batch_id, reason_code)" in macro
    assert "DO NOTHING" in macro
