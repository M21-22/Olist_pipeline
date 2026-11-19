"""Certified-batch incremental dbt smoke test on disposable warehouse_test only."""
import os
import subprocess
from datetime import datetime, timezone, timedelta
from pathlib import Path
from uuid import uuid4

import pytest
import psycopg

if os.environ.get("OLIST_RUN_DB_INTEGRATION") != "1" or os.environ.get("OLIST_TEST_DBT") != "1":
    pytest.skip("Disposable dbt integration test is disabled", allow_module_level=True)

from airflow.providers.postgres.hooks.postgres import PostgresHook


@pytest.mark.integration
def test_incremental_orders_replay_and_tombstone(tmp_path: Path):
    admin = PostgresHook(postgres_conn_id="olist_test_admin")
    with admin.get_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT current_database()")
        assert cur.fetchone()[0] == "warehouse_test", "Refusing to mutate non-test warehouse"
        cur.execute("SELECT to_regclass('audit.dbt_staging_progress')")
        assert cur.fetchone()[0] is not None

    profile = tmp_path / "profiles.yml"
    profile.write_text('olist:\n  target: dev\n  outputs:\n    dev:\n      type: postgres\n      host: test-db\n      port: 5432\n      dbname: warehouse_test\n      user: dbt_runner\n      password: "{{ env_var(\'DBT_RUNNER_PASSWORD\') }}"\n      schema: staging\n      threads: 1\n')
    dbt = "/home/airflow/dbt-venv/bin/dbt"

    def run_dbt():
        p = subprocess.run(
            [dbt, "run", "--project-dir", "/opt/airflow/dbt",
             "--profiles-dir", str(tmp_path), "--select", "stg_orders",
             "--target-path", str(tmp_path / "target"),
             "--log-path", str(tmp_path / "logs")],
            capture_output=True, text=True, timeout=120, check=False,
            env=os.environ.copy(),
        )
        assert p.returncode == 0, p.stdout[-5000:] + "\n" + p.stderr[-3000:]

    test_id = f"dbt53-{uuid4().hex}"
    order_id = test_id + "-order"
    b1, b2, b3 = [test_id + "-batch-" + str(i) for i in (1, 2, 3)]
    t = datetime(2017, 3, 1, tzinfo=timezone.utc)

    def add_batch(batch, status, source_updated_at, certified_at):
        with admin.get_conn() as conn, conn.cursor() as cur:
            cur.execute("""INSERT INTO audit.landed_batches
                (batch_id, table_name, extraction_mode, logical_interval_end,
                 parquet_key, manifest_key, row_count)
                VALUES (%s,'orders','incremental',%s,%s,%s,1)""",
                (batch, certified_at, batch + '.parquet', batch + '.json'))
            cur.execute("""INSERT INTO audit.batch_registry
                (batch_id, table_name, status, expected_rows, actual_rows, certified_at)
                VALUES (%s,'orders','CERTIFIED',1,1,%s)""", (batch, certified_at))
            cur.execute("""INSERT INTO raw.orders
                (order_id, order_status, updated_at, _batch_id, _source_file, _row_hash)
                VALUES (%s,%s,%s,%s,%s,%s)""",
                (order_id, status, source_updated_at, batch, batch + '.parquet', batch + '-hash'))

    def current_status():
        with psycopg.connect(
            host="test-db",
            port=5432,
            dbname="warehouse_test",
            user="dbt_runner",
            password=os.environ["DBT_RUNNER_PASSWORD"],
        ) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT order_status
                    FROM staging.stg_orders
                    WHERE order_id = %s
                    """,
                    (order_id,),
                )
                row = cur.fetchone()
                return row[0] if row else None

    # Test database is disposable, with no production data.
    add_batch(b1, "created", t, t)
    run_dbt()
    assert current_status() == "created"
    run_dbt()  # no new certifications; cursor must not regress
    assert current_status() == "created"
    add_batch(b2, "delivered", t + timedelta(days=3), t + timedelta(days=4))
    run_dbt()
    assert current_status() == "delivered"
    # A later certification of an older business version must not overwrite latest.
    add_batch(b3, "shipped", t + timedelta(days=1), t + timedelta(days=5))
    run_dbt()
    assert current_status() == "delivered"

    with admin.get_conn() as conn, conn.cursor() as cur:
        cur.execute("""INSERT INTO audit.deleted_keys(source_table, business_key, deleted_at)
            VALUES ('orders', %s, now())""", (order_id,))
    run_dbt()
    assert current_status() is None
