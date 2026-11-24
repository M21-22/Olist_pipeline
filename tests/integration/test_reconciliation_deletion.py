"""Repair lifecycle on disposable PostgreSQL with in-memory object storage."""
import hashlib
import json
import uuid
from datetime import UTC, datetime, timedelta
from io import BytesIO
from pathlib import Path

import pytest
from airflow.providers.postgres.hooks.postgres import PostgresHook
from botocore.response import StreamingBody
from include import load, reconcile, repair
from include.common import canonical_json

pytestmark = pytest.mark.integration


def test_continuous_reconciliation_repairs_backdated_updates_deletes_and_reinsertions(monkeypatch):
    monkeypatch.setattr(reconcile, "SOURCE_CONN_ID", "olist_test_source")
    monkeypatch.setattr(reconcile, "RECONCILE_CONN_ID", "olist_test_reconcile")
    identity = "olist_test_loader"
    monkeypatch.setattr(repair, "LOADER_CONN_ID", identity)
    monkeypatch.setattr(load, "LOADER_CONN_ID", identity)
    objects = {}

    class RepairStorage:
        def upload_file(self, path, bucket, key):
            objects[bucket, key] = Path(path).read_bytes()

        def get_object(self, *, Bucket, Key):
            data = objects[Bucket, Key]
            return {"Body": StreamingBody(BytesIO(data), len(data))}

        def put_object(self, *, Bucket, Key, Body):
            objects[Bucket, Key] = Body

    monkeypatch.setattr(repair.S3Hook, "get_conn", lambda _: RepairStorage())
    admin = PostgresHook(postgres_conn_id="olist_test_admin")
    with admin.get_conn() as check_conn:
        with check_conn.cursor() as check_cur:
            check_cur.execute("SELECT current_database()")
            assert check_cur.fetchone()[0] == "warehouse_test", "Refusing non-test database"
        check_conn.rollback()
    key = "ci-deleted-" + uuid.uuid4().hex
    batch_id = "ci-certified-" + uuid.uuid4().hex
    updated = datetime(2026, 11, 15, 12, tzinfo=UTC)
    checkpoint = datetime(2026, 11, 16, 18, tzinfo=UTC)
    row = {
        "customer_id": key, "customer_unique_id": key, "customer_zip_code_prefix": "12345",
        "customer_city": "test", "customer_state": "SP", "updated_at": updated,
    }
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
    def sql(statement, params=()):
        with admin.get_conn() as conn, conn.cursor() as cur:
            cur.execute(statement, params)
            return cur.fetchall() if cur.description else None

    try:
        baseline = reconcile.compare_table("customers", mode="continuous")
        assert baseline["status"] == "MATCH", baseline
        backdated = updated - timedelta(days=1)
        sql("UPDATE shop.customers SET customer_city = 'rio', updated_at = %s", (backdated,))
        changed = repair.repair_table("customers")
        assert changed["status"] == "MATCH" and changed["repair_batch_id"]
        assert sql("SELECT updated_at FROM raw.customers WHERE _batch_id = %s",
                   (changed["repair_batch_id"],)) == [(backdated,)]
        assert "repair_batch_id" not in repair.repair_table("customers")
        sql("DELETE FROM shop.customers WHERE customer_id = %s", (key,))
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

        deleted = repair.repair_table("customers")
        assert deleted["status"] == "MATCH"
        assert sql("SELECT business_key FROM audit.deleted_keys WHERE source_table = 'customers'") == [(key,)]
        delete_manifest = json.loads(objects["landing", f"customers/repair/{deleted['repair_batch_id']}.manifest.json"])
        assert delete_manifest["deleted_keys"] == [key]
        assert delete_manifest["row_count"] == 0

        row.update(customer_city="salvador", updated_at=backdated)
        sql("INSERT INTO shop.customers VALUES (%s,%s,%s,%s,%s,%s)", tuple(row.values()))
        with monkeypatch.context() as injection:
            def fail_gate2(**_):
                raise RuntimeError("injected repair Gate 2 failure")
            injection.setattr(repair, "validate_loaded_batch", fail_gate2)
            with pytest.raises(RuntimeError, match="injected repair Gate 2 failure"):
                repair.repair_table("customers")
        assert sql("SELECT business_key FROM audit.deleted_keys WHERE source_table = 'customers'") == [(key,)]
        assert sql("SELECT status, unrepaired_count FROM audit.reconciliation ORDER BY reconciliation_id DESC LIMIT 1") == [
            ("MISMATCH", 1),
        ]

        revived = repair.repair_table("customers")
        assert revived["status"] == "MATCH" and revived["repair_batch_id"]
        assert sql("SELECT business_key FROM audit.deleted_keys WHERE source_table = 'customers'") == []
        assert reconcile.compare_table("customers", mode="full")["status"] == "MATCH"
        assert sql("SELECT certified_high_watermark, last_certified_batch_id FROM audit.certified_watermarks "
                   "WHERE table_name = 'customers'") == [(checkpoint, batch_id)]
    finally:
        with admin.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM shop.customers WHERE customer_id=%s", (key,))
                repair_ids = [
                    artifact.split("/")[-1].split(".")[0] for _, artifact in objects
                ]
                batches = [batch_id, *set(repair_ids)]
                cur.execute("DELETE FROM raw.customers WHERE _batch_id = ANY(%s)", (batches,))
                cur.execute("DELETE FROM audit.deleted_keys WHERE source_table = 'customers' AND business_key = %s", (key,))
                cur.execute("DELETE FROM audit.dq_results WHERE batch_id = ANY(%s)", (batches,))
                cur.execute("DELETE FROM audit.reconciliation WHERE table_name='customers'")
                cur.execute("DELETE FROM audit.certified_watermarks WHERE last_certified_batch_id=%s", (batch_id,))
                cur.execute("DELETE FROM audit.batch_registry WHERE batch_id = ANY(%s)", (batches,))
                cur.execute("DELETE FROM audit.landed_batches WHERE batch_id = ANY(%s)", (batches,))
            conn.commit()
