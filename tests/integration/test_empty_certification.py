"""Opt-in isolated PostgreSQL test: empty Parquet Gate 1 -> Gate 2 -> certification.

Requires Airflow connections olist_test_loader (etl_loader on warehouse_test)
and olist_test_admin (fixture role on warehouse_test). Does not touch S3.
"""
import json
import os
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

if os.getenv("OLIST_RUN_DB_INTEGRATION") != "1":
    pytest.skip("Set OLIST_RUN_DB_INTEGRATION=1", allow_module_level=True)

import pyarrow.parquet as pq
from airflow.providers.postgres.hooks.postgres import PostgresHook
from include import load
from include.common import expected_schema, sha256_file
from include.gates.gate1 import validate_landed_batch

END = datetime(1900, 1, 1, tzinfo=timezone.utc)
START = END - timedelta(minutes=15)
TABLE = "orders"
DB_NAME = "warehouse_test"


def get_connection(conn_id):
    hook = PostgresHook(postgres_conn_id=conn_id)
    conn = hook.get_conn()
    with conn.cursor() as cur:
        cur.execute("SELECT current_database(), current_user")
        db, user = cur.fetchone()
    conn.rollback()
    assert db == DB_NAME, f"Refusing to run against {db!r}"
    return conn, user


@pytest.mark.integration
def test_empty_batch_gate1_gate2_and_certification(tmp_path: Path, monkeypatch):
    admin_id = os.getenv("OLIST_TEST_ADMIN_CONN_ID", "olist_test_admin")
    admin, admin_user = get_connection(admin_id)
    loader_conn, loader_user = get_connection("olist_test_loader")
    admin.close()
    loader_conn.close()
    assert admin_user == "olist_test_admin"
    assert loader_user == "etl_loader"
    monkeypatch.setattr(load, "LOADER_CONN_ID", "olist_test_loader")

    batch_id = f"c-empty-{uuid.uuid4().hex}"
    parquet_key = f"__integration__/{batch_id}.parquet"
    path = tmp_path / "empty.parquet"
    csv_path = tmp_path / "empty.csv"
    schema = expected_schema(TABLE)
    with pq.ParquetWriter(str(path), schema, compression="zstd", version="2.6"):
        pass
    manifest = {
        "table": TABLE, "batch_id": batch_id, "mode": "incremental",
        "interval_start": START.isoformat(), "interval_end": END.isoformat(),
        "overlap_minutes": 10, "sha256": sha256_file(path), "row_count": 0,
        "schema_fingerprint": __import__("hashlib").sha256(schema.serialize().to_pybytes()).hexdigest(),
        "null_counts": {name: 0 for name in schema.names},
        "control_totals": {}, "min_updated_at": None, "max_updated_at": None,
    }
    checks = []
    parquet = validate_landed_batch(
        manifest=manifest, parquet_path=path, batch_id=batch_id, table=TABLE,
        mode="incremental", interval_end=END, check_results=checks,
    )
    assert parquet.metadata.num_rows == 0
    assert checks and all(check["status"] == "PASS" for check in checks)
    columns = load._write_copy_csv(parquet, batch_id, parquet_key, csv_path)
    assert csv_path.read_bytes() == b""

    admin_hook = PostgresHook(postgres_conn_id=admin_id)
    def fetchone(query, params=()):
        with admin_hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                return cur.fetchone()

    prior_wm = fetchone(
        "SELECT certified_high_watermark, last_certified_batch_id "
        "FROM audit.certified_watermarks WHERE table_name = %s", (TABLE,)
    )
    if prior_wm and (prior_wm[0] is None or prior_wm[0] < END):
        pytest.skip("Checkpoint precondition unsuitable: would alter preexisting state")
    older_uncertified = fetchone(
        """SELECT COUNT(*) FROM audit.landed_batches b
           LEFT JOIN audit.batch_registry r ON r.batch_id=b.batch_id
           WHERE b.table_name=%s AND b.extraction_mode='incremental'
             AND b.logical_interval_end < %s AND r.status IS DISTINCT FROM 'CERTIFIED'""",
        (TABLE, END)
    )[0]
    if older_uncertified:
        pytest.skip("Older uncertified batches block this interval")

    seeded = False
    try:
        with admin_hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO audit.landed_batches
                       (batch_id, table_name, extraction_mode, logical_interval_start,
                        logical_interval_end, parquet_key, manifest_key, row_count)
                       VALUES (%s, %s, 'incremental', %s, %s, %s, %s, 0)""",
                    (batch_id, TABLE, START, END, parquet_key, parquet_key + ".manifest.json")
                )
            conn.commit()
        seeded = True
        load._load_transaction(
            table=TABLE, batch_id=batch_id, interval_end=END,
            manifest=manifest, columns=columns, copy_path=csv_path,
        )
        assert fetchone(
            "SELECT COUNT(*) FROM raw.orders WHERE _batch_id=%s", (batch_id,)
        )[0] == 0
        assert fetchone(
            "SELECT status, actual_rows FROM audit.batch_registry WHERE batch_id=%s",
            (batch_id,)
        ) == ("CERTIFIED", 0)
        assert fetchone(
            """SELECT status, failed_count FROM audit.dq_results
               WHERE batch_id=%s AND gate='GATE_2' AND check_name='load_reconciliation'""",
            (batch_id,),
        ) == ("PASS", 0)
        new_wm = fetchone(
            "SELECT certified_high_watermark, last_certified_batch_id "
            "FROM audit.certified_watermarks WHERE table_name=%s", (TABLE,)
        )
        assert new_wm == ((END, batch_id) if prior_wm is None else prior_wm)
    finally:
        if seeded:
            with admin_hook.get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("DELETE FROM raw.orders WHERE _batch_id=%s", (batch_id,))
                    cur.execute("DELETE FROM audit.dq_results WHERE batch_id=%s", (batch_id,))
                    cur.execute("DELETE FROM audit.batch_registry WHERE batch_id=%s", (batch_id,))
                    cur.execute("DELETE FROM audit.landed_batches WHERE batch_id=%s", (batch_id,))
                    if prior_wm is None:
                        cur.execute(
                            "DELETE FROM audit.certified_watermarks "
                            "WHERE table_name=%s AND last_certified_batch_id=%s",
                            (TABLE, batch_id),
                        )
                conn.commit()
