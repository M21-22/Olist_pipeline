"""Opt-in PostgreSQL integration test for atomic Gate 2 recovery.

Run only against a disposable warehouse or while pipeline writers are stopped.
Requires the Airflow connection OLIST_TEST_ADMIN_CONN_ID with permissions to
insert/delete synthetic audit.landed_batches and clean up dependent test rows.
The production loader still uses its normal `olist_loader` connection.

Example:
    OLIST_RUN_DB_INTEGRATION=1 OLIST_TEST_ADMIN_CONN_ID=<test_admin_conn> \
        python -m pytest -v /tmp/test_gate2_transaction.py
"""

import csv
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pytest

if os.environ.get("OLIST_RUN_DB_INTEGRATION") != "1":
    pytest.skip("Live database integration disabled (set OLIST_RUN_DB_INTEGRATION=1)", allow_module_level=True)

from airflow.providers.postgres.hooks.postgres import PostgresHook
from include import load
from include.gates.gate2 import Gate2Failure

@pytest.fixture(autouse=True)
def isolated_loader(monkeypatch):
    monkeypatch.setattr(load, "LOADER_CONN_ID", "olist_test_loader")

@pytest.mark.integration
def test_failed_gate2_rollback_then_successful_retry(tmp_path: Path):
    admin_conn_id = os.environ.get("OLIST_TEST_ADMIN_CONN_ID")
    if not admin_conn_id:
        pytest.skip("Provide OLIST_TEST_ADMIN_CONN_ID for disposable test data setup/cleanup")

    table = "orders"
    end = datetime(1900, 1, 1, tzinfo=timezone.utc)
    batch_id = f"b2-integration-{uuid.uuid4().hex}"
    order_id = f"b2-integration-{uuid.uuid4().hex}"
    parquet_key = f"__integration__/{batch_id}.parquet"
    manifest_key = f"__integration__/{batch_id}.json"
    copy_path = tmp_path / "rows.csv"

    # An intentionally minimal PostgreSQL COPY CSV. Nullable order fields
    # that are not in `columns` receive their defaults (NULL).
    with copy_path.open("w", newline="", encoding="utf-8") as stream:
        csv.writer(stream).writerow([order_id, batch_id, parquet_key, "test-hash"])

    admin_hook = PostgresHook(postgres_conn_id=admin_conn_id)

    def fetchone(sql, params=()):
        with admin_hook.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, params)
                return cursor.fetchone()

    # Do not advance or rewrite a real certification checkpoint, even if this
    # test is accidentally pointed at a non-disposable warehouse.
    existing_wm = fetchone(
        "SELECT certified_high_watermark, last_certified_batch_id "
        "FROM audit.certified_watermarks WHERE table_name = %s", (table,)
    )
    if existing_wm and existing_wm[0] is None:
        pytest.skip("Existing NULL checkpoint: avoid modifying preexisting state")
    if existing_wm and existing_wm[0] < end:
        pytest.skip("Existing checkpoint predates synthetic interval; refusing to change it")

    older_uncertified = fetchone(
        """SELECT COUNT(*) FROM audit.landed_batches AS b
           LEFT JOIN audit.batch_registry AS r ON b.batch_id = r.batch_id
           WHERE b.table_name = %s AND b.extraction_mode = 'incremental'
             AND b.logical_interval_end < %s
             AND r.status IS DISTINCT FROM 'CERTIFIED'""",
        (table, end),
    )[0]
    if older_uncertified:
        pytest.skip("Older uncertified intervals exist; test must not bypass ordering")

    # Confirm the DDL migration required by both Gate 1 and Gate 2 upserts.
    unique_exists = fetchone(
        """SELECT COUNT(*) FROM pg_constraint c
           JOIN pg_class t ON t.oid = c.conrelid
           JOIN pg_namespace n ON n.oid = t.relnamespace
           WHERE n.nspname = 'audit' AND t.relname = 'dq_results'
             AND c.contype = 'u'
             AND (SELECT array_agg(a.attname ORDER BY a.attname)
                  FROM unnest(c.conkey) AS k(attnum)
                  JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = k.attnum)
                 = ARRAY['batch_id','check_name','gate']::name[]"""
    )[0]
    assert unique_exists, "Apply UNIQUE(batch_id, gate, check_name) to running DB first"

    seeded = False
    try:
        with admin_hook.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """INSERT INTO audit.landed_batches
                       (batch_id, table_name, extraction_mode, logical_interval_end,
                        parquet_key, manifest_key, row_count)
                       VALUES (%s, %s, 'incremental', %s, %s, %s, 1)""",
                    (batch_id, table, end, parquet_key, manifest_key),
                )
            conn.commit()
        seeded = True

        def execute_load(expected_rows):
            return load._load_transaction(
                table=table,
                batch_id=batch_id,
                interval_end=end,
                manifest={"row_count": expected_rows, "control_totals": {}},
                columns=["order_id"],
                copy_path=copy_path,
            )

        # 1. Deliberate mismatch: actual 1 row vs expected 2.
        with pytest.raises(Gate2Failure, match="expected 2 rows, found 1"):
            execute_load(expected_rows=2)

        assert fetchone(
            "SELECT COUNT(*) FROM raw.orders WHERE _batch_id = %s", (batch_id,)
        )[0] == 0, "COPY must be rolled back"
        assert fetchone(
            "SELECT status, reason FROM audit.batch_registry WHERE batch_id = %s",
            (batch_id,),
        )[0] == "FAILED", "Failure audit must survive the rollback"
        assert fetchone(
            """SELECT status, failed_count FROM audit.dq_results
               WHERE batch_id = %s AND gate = 'GATE_2'
                 AND check_name = 'load_reconciliation'""", (batch_id,)
        ) == ("FAIL", 1)
        assert fetchone(
            "SELECT certified_high_watermark, last_certified_batch_id "
            "FROM audit.certified_watermarks WHERE table_name = %s", (table,)
        ) == existing_wm, "Failure must not advance checkpoint"

        # 2. Fix the expected count: retry same batch ID successfully.
        execute_load(expected_rows=1)
        assert fetchone(
            "SELECT COUNT(*) FROM raw.orders WHERE _batch_id = %s", (batch_id,)
        )[0] == 1
        assert fetchone(
            "SELECT status, reason, actual_rows FROM audit.batch_registry WHERE batch_id = %s",
            (batch_id,),
        ) == ("CERTIFIED", None, 1)
        assert fetchone(
            """SELECT status, failed_count, details FROM audit.dq_results
               WHERE batch_id = %s AND gate = 'GATE_2'
                 AND check_name = 'load_reconciliation'""", (batch_id,)
        ) == ("PASS", 0, {})
        assert fetchone(
            """SELECT COUNT(*) FROM audit.dq_results
               WHERE batch_id = %s AND gate = 'GATE_2'
                 AND check_name = 'load_reconciliation'""", (batch_id,)
        )[0] == 1, "Retry should upsert, not duplicate the check"

        new_wm = fetchone(
            "SELECT certified_high_watermark, last_certified_batch_id "
            "FROM audit.certified_watermarks WHERE table_name = %s", (table,)
        )
        if existing_wm is None:
            assert new_wm == (end, batch_id)
        else:
            assert new_wm == existing_wm, "Old interval must not regress checkpoint"
    finally:
        if seeded:
            with admin_hook.get_conn() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("DELETE FROM raw.orders WHERE _batch_id = %s", (batch_id,))
                    cursor.execute("DELETE FROM audit.dq_results WHERE batch_id = %s", (batch_id,))
                    cursor.execute("DELETE FROM audit.batch_registry WHERE batch_id = %s", (batch_id,))
                    cursor.execute("DELETE FROM audit.landed_batches WHERE batch_id = %s", (batch_id,))
                    # Only remove a checkpoint if this test created it and
                    # no other actor has since certified a newer interval.
                    if existing_wm is None:
                        cursor.execute(
                            """DELETE FROM audit.certified_watermarks
                               WHERE table_name = %s AND last_certified_batch_id = %s""",
                            (table, batch_id),
                        )
                conn.commit()
