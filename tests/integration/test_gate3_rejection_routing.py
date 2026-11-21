"""Validate current-state rejection routing and subsequent corrections."""
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
import subprocess
from uuid import uuid4

import psycopg
import pytest
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta, timezone

@pytest.mark.integration
def test_rejection_routes_current_version_and_correction(tmp_path: Path):
    admin = PostgresHook(postgres_conn_id="olist_test_admin")
    with admin.get_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT current_database()")
        assert cur.fetchone()[0] == "warehouse_test"

    (tmp_path / "profiles.yml").write_text(
        "olist:\n  target: dev\n  outputs:\n    dev:\n      type: postgres\n"
        "      host: test-db\n      port: 5432\n      dbname: warehouse_test\n"
        "      user: dbt_runner\n"
        '      password: "{{ env_var(\'DBT_RUNNER_PASSWORD\') }}"\n'
        "      schema: staging\n      threads: 1\n"
    )
    dbt = "/home/airflow/dbt-venv/bin/dbt"

    def invoke(*args):
        p = subprocess.run(
            [dbt, *args, "--project-dir", "/opt/airflow/dbt",
             "--profiles-dir", str(tmp_path),
             "--target-path", str(tmp_path / "target"),
             "--log-path", str(tmp_path / "logs")],
            capture_output=True, text=True, timeout=180, check=False,
            env=os.environ.copy(),
        )
        assert p.returncode == 0, p.stdout[-5000:] + "\n" + p.stderr[-3000:]

    fixture_id = uuid4().hex
    token = f"dq53-{fixture_id}"
    order_id = token + "-order"
    # The landed-batch uniqueness constraint also includes the interval end.
    t = datetime(2017, 3, 10, tzinfo=timezone.utc) + timedelta(
        microseconds=int(fixture_id[:12], 16)
    )

    def insert_version(
        suffix,
        status,
        source_updated_at,
        certified_at,
        interval_end,
    ):
        batch = token + suffix

        with admin.get_conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO audit.landed_batches
                    (batch_id, table_name, extraction_mode,
                    logical_interval_end, parquet_key, manifest_key, row_count)
                VALUES (%s, 'orders', 'incremental', %s, %s, %s, 1)
                """,
                (
                    batch,
                    interval_end,
                    batch + ".parquet",
                    batch + ".json",
                ),
            )

            cur.execute(
                """
                INSERT INTO audit.batch_registry
                    (batch_id, table_name, status,
                    expected_rows, actual_rows, certified_at)
                VALUES (%s, 'orders', 'CERTIFIED', 1, 1, %s)
                """,
                (batch, certified_at),
            )

            cur.execute(
                """
                INSERT INTO raw.orders
                    (order_id, order_status, updated_at,
                    _batch_id, _source_file, _row_hash)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (
                    order_id,
                    status,
                    source_updated_at,
                    batch,
                    batch + ".parquet",
                    batch + "-hash",
                ),
            )

        return batch

    invalid_batch = insert_version(
        suffix="-bad",
        status="not_a_status",
        source_updated_at=t,
        certified_at=t + timedelta(minutes=2),
        interval_end=t + timedelta(minutes=15),
    )
    invoke('run', '--full-refresh', '--select', 'staging', 'rejected_rows_candidates')

    def query_runner(sql, params):
        with psycopg.connect(host='test-db', dbname='warehouse_test',
                             user='dbt_runner', password=os.environ['DBT_RUNNER_PASSWORD']) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                return cur.fetchone()

    def status():
        return query_runner(
            'SELECT is_valid, source_batch_id FROM staging.stg_orders WHERE order_id=%s',
            (order_id,),
        )

    assert status() == (False, invalid_batch)
    invoke('run-operation', 'sync_rejected_rows')
    invoke('run-operation', 'sync_rejected_rows')
    with admin.get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """SELECT count(*) FROM quarantine.rejected_rows
               WHERE source_table='orders' AND business_key=%s
                 AND batch_id=%s AND reason_code='unknown_order_status'""",
            (order_id, invalid_batch),
        )
        assert cur.fetchone()[0] == 1

    valid_batch = insert_version(
        suffix="-good",
        status="created",
        source_updated_at=t + timedelta(minutes=15),
        certified_at=t + timedelta(minutes=17),
        interval_end=t + timedelta(minutes=30),
    )
    invoke('run', '--select', 'stg_orders', 'rejected_rows_candidates')
    assert status() == (True, valid_batch)
    assert query_runner(
        "SELECT count(*) FROM staging.rejected_rows_candidates WHERE source_table='orders' AND business_key=%s",
        (order_id,),
    )[0] == 0
