"""Real PostgreSQL deletion detection, isolated source and warehouse schema."""
import hashlib
import uuid
from datetime import datetime, timezone

import pytest
from airflow.providers.postgres.hooks.postgres import PostgresHook
from include import reconcile
from include.common import canonical_json

pytestmark = pytest.mark.integration


def test_continuous_reconciliation_detects_and_audits_source_deletion(monkeypatch):
    monkeypatch.setattr(reconcile, "SOURCE_CONN_ID", "olist_test_source")
    monkeypatch.setattr(reconcile, "RECONCILE_CONN_ID", "olist_test_reconcile")
    admin = PostgresHook(postgres_conn_id="olist_test_admin")
    with admin.get_conn() as check_conn:
        with check_conn.cursor() as check_cur:
            check_cur.execute("SELECT current_database()")
            assert check_cur.fetchone()[0] == "warehouse_test", "Refusing non-test database"
        check_conn.rollback()
    key = "ci-deleted-" + uuid.uuid4().hex
    batch_id = "ci-certified-" + uuid.uuid4().hex
    updated = datetime(2026, 11, 15, 12, tzinfo=timezone.utc)
    checkpoint = datetime(2026, 11, 16, 18, tzinfo=timezone.utc)
    row = dict(customer_id=key, customer_unique_id=key, customer_zip_code_prefix="12345",
               customer_city="test", customer_state="SP", updated_at=updated)
    digest = hashlib.sha256(canonical_json(row)).hexdigest()
    # The schema is entirely disposable; guarantee one current certified record.
    with admin.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM shop.customers")
            cur.execute("DELETE FROM raw.customers")
            cur.execute("DELETE FROM audit.certified_watermarks WHERE table_name='customers'")
            cur.execute("DELETE FROM audit.reconciliation WHERE table_name='customers'")
            cur.execute(
                "INSERT INTO shop.customers VALUES (%s,%s,%s,%s,%s,%s)", tuple(row.values())
            )
            cur.execute(
                """INSERT INTO audit.landed_batches
                 (batch_id,table_name,extraction_mode,logical_interval_end,parquet_key,manifest_key,row_count)
                 VALUES (%s,'customers','incremental',%s,'ci.parquet','ci.json',1)""",
                (batch_id, checkpoint),
            )
            cur.execute(
                """INSERT INTO audit.batch_registry
                 (batch_id,table_name,status,expected_rows) VALUES (%s,'customers','CERTIFIED',1)""",
                (batch_id,),
            )
            cur.execute(
                """INSERT INTO audit.certified_watermarks
                 (table_name,certified_high_watermark,last_certified_batch_id)
                 VALUES ('customers',%s,%s)""", (checkpoint, batch_id)
            )
            cur.execute(
                """INSERT INTO raw.customers
                 (customer_id,customer_unique_id,customer_zip_code_prefix,customer_city,
                  customer_state,updated_at,_batch_id,_source_file,_row_hash)
                 VALUES (%s,%s,%s,%s,%s,%s,%s,'ci.parquet',%s)""",
                (*tuple(row.values()), batch_id, digest),
            )
        conn.commit()
    try:
        baseline = reconcile.compare_table("customers", mode="continuous")
        assert baseline["status"] == "MATCH", baseline
        with admin.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM shop.customers WHERE customer_id=%s", (key,))
            conn.commit()
        with pytest.raises(reconcile.ReconciliationMismatch, match="deleted=1"):
            reconcile.compare_table("customers", mode="continuous")
        with admin.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """SELECT status, mismatch_count, details FROM audit.reconciliation
                       WHERE table_name='customers' ORDER BY reconciliation_id DESC LIMIT 1"""
                )
                status, mismatches, details = cur.fetchone()
                assert status == "MISMATCH" and mismatches == 1
                assert details["deleted_count"] == 1
                assert key in details["deleted"]
            conn.rollback()
    finally:
        with admin.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM shop.customers WHERE customer_id=%s", (key,))
                cur.execute("DELETE FROM raw.customers WHERE _batch_id=%s", (batch_id,))
                cur.execute("DELETE FROM audit.reconciliation WHERE table_name='customers'")
                cur.execute("DELETE FROM audit.certified_watermarks WHERE last_certified_batch_id=%s", (batch_id,))
                cur.execute("DELETE FROM audit.batch_registry WHERE batch_id=%s", (batch_id,))
                cur.execute("DELETE FROM audit.landed_batches WHERE batch_id=%s", (batch_id,))
            conn.commit()
