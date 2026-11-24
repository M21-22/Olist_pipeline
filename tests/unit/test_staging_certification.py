"""Fast structural guardrails; database semantics are verified by integration test."""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
MODELS = ('customers', 'orders', 'order_items', 'order_payments',
          'order_reviews', 'products', 'sellers')


def test_all_mutable_models_have_serialized_membership_hooks():
    for table in MODELS:
        sql = (ROOT / 'dbt' / 'models' / 'staging' / f'stg_{table}.sql').read_text()
        assert 'staging_lock()' in sql, table
        assert f"staging_prepare('{table}')" in sql, table
        assert f"staging_capture_pending('{table}')" in sql, table
        assert f"staging_mark_processed('{table}')" in sql, table
        assert 'staging_finalize()' in sql, table
        assert f"staging_incremental_key_filter('{table}'" in sql, table
        assert sql.index('staging_lock()') < sql.index(f"staging_prepare('{table}')")
        assert sql.index(f"staging_mark_processed('{table}')") < sql.index('staging_finalize()')


def test_incremental_selection_uses_unprocessed_certification_membership():
    sql = (ROOT / 'dbt/macros/staging_incremental.sql').read_text()
    assert 'pg_advisory_xact_lock' in sql
    assert 'dbt_staging_processed_batches' in sql
    assert 'NOT EXISTS' in sql
    assert 'CREATE TEMP TABLE olist_staging_pending_batches ON COMMIT DROP' in sql
    assert 'pg_temp.olist_staging_pending_batches' in sql
    assert 'progress.last_certified_at' not in sql.split('macro staging_incremental_key_filter')[1]


def test_processed_batch_membership_key_and_grants_are_scoped():
    init = ROOT / 'infra/postgres/warehouse-init'
    audit = (init / '04-audit.sql').read_text()
    grants = (init / '06-grants.sql').read_text()
    membership = audit.split('CREATE TABLE audit.dbt_staging_processed_batches (', 1)[1].split(');', 1)[0]
    assert 'PRIMARY KEY (model_name, batch_id)' in membership
    assert 'GRANT SELECT, INSERT ON audit.dbt_staging_processed_batches TO dbt_runner;' in grants
