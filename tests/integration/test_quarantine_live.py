"""Real S3 and Postgres quarantine test; disposable Compose services only."""
import json
import uuid
from datetime import datetime, timezone

import pytest
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from include import load

pytestmark = pytest.mark.integration


def test_gate1_failure_creates_real_quarantine_artifacts_and_registry(monkeypatch):
    batch_id = "ci-quarantine-" + uuid.uuid4().hex
    table = "orders"
    parquet_key = f"{table}/ci/{batch_id}.parquet"
    manifest_key = f"{table}/ci/{batch_id}.manifest.json"
    end = datetime(2026, 11, 16, 18, 0, tzinfo=timezone.utc)

    # Never touch production Airflow/Postgres connections.
    monkeypatch.setattr(load, "LOADER_CONN_ID", "olist_test_admin")
    hook = PostgresHook(postgres_conn_id="olist_test_admin")
    with hook.get_conn() as check_conn:
        with check_conn.cursor() as check_cur:
            check_cur.execute("SELECT current_database()")
            assert check_cur.fetchone()[0] == "warehouse_test", "Refusing non-test database"
        check_conn.rollback()
    s3 = S3Hook(aws_conn_id="olist_s3").get_conn()
    for bucket in ("landing", "quarantine"):
        try:
            s3.create_bucket(Bucket=bucket)
        except s3.exceptions.BucketAlreadyOwnedByYou:
            pass

    manifest = {"row_count": 1, "control_totals": {}}
    s3.put_object(Bucket="landing", Key=parquet_key, Body=b"not a valid parquet file")
    s3.put_object(Bucket="landing", Key=manifest_key, Body=json.dumps(manifest).encode())
    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO audit.landed_batches
                   (batch_id, table_name, extraction_mode, logical_interval_end,
                    parquet_key, manifest_key, row_count)
                   VALUES (%s, %s, 'incremental', %s, %s, %s, 1)""",
                (batch_id, table, end, parquet_key, manifest_key),
            )
        conn.commit()
    prefix = f"{table}/{batch_id}/"
    try:
        with pytest.raises(load.Gate1Failure):
            load.load_batch(batch_id)
        for key in ("batch.parquet", "manifest.json", "reason.json"):
            s3.head_object(Bucket="quarantine", Key=prefix + key)
        assert s3.get_object(Bucket="quarantine", Key=prefix + "batch.parquet")["Body"].read() == b"not a valid parquet file"
        reason = json.loads(s3.get_object(Bucket="quarantine", Key=prefix + "reason.json")["Body"].read())
        assert reason["batch_id"] == batch_id and reason["gate"] == "GATE_1"
        s3.head_object(Bucket="landing", Key=parquet_key)
        s3.head_object(Bucket="landing", Key=manifest_key)
        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT status FROM audit.batch_registry WHERE batch_id=%s", (batch_id,))
                assert cur.fetchone() == ("QUARANTINED",)
                cur.execute("SELECT check_name, status FROM audit.dq_results WHERE batch_id=%s AND gate='GATE_1'", (batch_id,))
                assert any(status == "FAIL" for _, status in cur.fetchall())
                cur.execute("SELECT COUNT(*) FROM raw.orders WHERE _batch_id=%s", (batch_id,))
                assert cur.fetchone()[0] == 0
            conn.rollback()
    finally:
        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM audit.dq_results WHERE batch_id=%s", (batch_id,))
                cur.execute("DELETE FROM audit.batch_registry WHERE batch_id=%s", (batch_id,))
                cur.execute("DELETE FROM audit.landed_batches WHERE batch_id=%s", (batch_id,))
            conn.commit()
        for bucket, keys in (("landing", [parquet_key, manifest_key]), ("quarantine", [prefix + x for x in ("batch.parquet", "manifest.json", "reason.json")])):
            for key in keys:
                s3.delete_object(Bucket=bucket, Key=key)
