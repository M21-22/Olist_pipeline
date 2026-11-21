"""Verify purchase-time customer history from certified records in the disposable warehouse."""

import os
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

import psycopg
import pytest
from airflow.providers.postgres.hooks.postgres import PostgresHook

pytestmark = pytest.mark.integration

if (
    os.environ.get("OLIST_RUN_DB_INTEGRATION") != "1"
    or os.environ.get("OLIST_TEST_DBT") != "1"
):
    pytest.skip("Disposable dbt integration is disabled", allow_module_level=True)


DBT = "/home/airflow/dbt-venv/bin/dbt"


def test_late_purchase_rekeys_existing_fact_and_matches_rebuild(tmp_path: Path):
    """A late purchase splits a range and re-keys an already loaded order."""
    admin = PostgresHook(postgres_conn_id="olist_test_admin")
    with admin.get_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT current_database()")
        assert cur.fetchone()[0] == "warehouse_test", "Refusing non-test database"

    (tmp_path / "profiles.yml").write_text(
        "olist:\n  target: dev\n  outputs:\n    dev:\n"
        "      type: postgres\n      host: test-db\n      port: 5432\n"
        "      dbname: warehouse_test\n      user: dbt_runner\n"
        '      password: "{{ env_var(\'DBT_RUNNER_PASSWORD\') }}"\n'
        "      schema: staging\n      threads: 1\n",
        encoding="utf-8",
    )

    def invoke(*args: str):
        result = subprocess.run(
            [
                DBT, *args,
                "--project-dir", "/opt/airflow/dbt",
                "--profiles-dir", str(tmp_path),
                "--target-path", str(tmp_path / "target"),
                "--log-path", str(tmp_path / "logs"),
            ],
            capture_output=True,
            text=True,
            timeout=240,
            check=False,
            env=os.environ.copy(),
        )
        assert result.returncode == 0, (
            f"dbt {' '.join(args)} failed:\n"
            f"{result.stdout[-8000:]}\n{result.stderr[-3000:]}"
        )

    def insert_certified_customer(batch_id: str, customer_id: str,
                                  customer_unique_id: str, city: str,
                                  updated_at: datetime, certified_at: datetime):
        with admin.get_conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO audit.landed_batches
                    (batch_id, table_name, extraction_mode, logical_interval_end,
                     parquet_key, manifest_key, row_count)
                VALUES (%s, 'customers', 'incremental', %s, %s, %s, 1)
                """,
                (batch_id, certified_at, batch_id + ".parquet", batch_id + ".json"),
            )
            cur.execute(
                """
                INSERT INTO audit.batch_registry
                    (batch_id, table_name, status, expected_rows, actual_rows, certified_at)
                VALUES (%s, 'customers', 'CERTIFIED', 1, 1, %s)
                """,
                (batch_id, certified_at),
            )
            cur.execute(
                """
                INSERT INTO raw.customers
                    (customer_id, customer_unique_id, customer_zip_code_prefix,
                     customer_city, customer_state, updated_at,
                     _batch_id, _source_file, _row_hash)
                VALUES (%s, %s, '01001', %s, 'SP', %s, %s, %s, %s)
                """,
                (customer_id, customer_unique_id, city, updated_at,
                 batch_id, batch_id + ".parquet", batch_id + "-hash"),
            )

    token = uuid4().hex
    person = f"scd2-{token}"
    purchased = datetime(2017, 3, 1, tzinfo=timezone.utc)
    certified = datetime(2020, 3, 1, tzinfo=timezone.utc) + timedelta(
        microseconds=int(token[:12], 16)
    )

    def insert_purchase(day: int, city: str, arrival: int) -> str:
        order_id = f"{person}-order-{day}"
        customer_id = f"{person}-customer-{day}"
        purchase_time = purchased + timedelta(days=day)
        certification_time = certified + timedelta(seconds=arrival)
        insert_certified_customer(
            f"{person}-customers-{day}", customer_id, person, city,
            purchase_time, certification_time,
        )
        batch_id = f"{person}-orders-{day}"
        with admin.get_conn() as conn, conn.cursor() as cur:
            cur.execute(
                """INSERT INTO audit.landed_batches
                    (batch_id, table_name, extraction_mode, logical_interval_end,
                     parquet_key, manifest_key, row_count)
                   VALUES (%s, 'orders', 'incremental', %s, %s, %s, 1)""",
                (batch_id, certification_time, batch_id + ".parquet", batch_id + ".json"),
            )
            cur.execute(
                """INSERT INTO audit.batch_registry
                    (batch_id, table_name, status, expected_rows, actual_rows, certified_at)
                   VALUES (%s, 'orders', 'CERTIFIED', 1, 1, %s)""",
                (batch_id, certification_time),
            )
            cur.execute(
                """INSERT INTO raw.orders
                    (order_id, customer_id, order_status, order_purchase_timestamp,
                     updated_at, _batch_id, _source_file, _row_hash)
                   VALUES (%s, %s, 'approved', %s, %s, %s, %s, %s)""",
                (order_id, customer_id, purchase_time, purchase_time,
                 batch_id, batch_id + ".parquet", batch_id + "-hash"),
            )
        return order_id

    def build(*options: str):
        invoke("run", *options, "--select",
               "+fct_orders")

    def read_state():
        with psycopg.connect(
            host="test-db", dbname="warehouse_test", user="dbt_runner",
            password=os.environ["DBT_RUNNER_PASSWORD"],
        ) as conn, conn.cursor() as cur:
            cur.execute(
                """SELECT customer_city, valid_from, valid_to, customer_sk
                   FROM marts.dim_customer WHERE customer_unique_id = %s
                   ORDER BY valid_from""",
                (person,),
            )
            versions = cur.fetchall()
            cur.execute(
                """SELECT order_id, customer_sk FROM marts.fct_orders
                   WHERE customer_unique_id = %s ORDER BY order_id""",
                (person,),
            )
            return versions, dict(cur.fetchall())

    first = insert_purchase(0, "Sao Paulo", 0)
    third = insert_purchase(2, "Sao Paulo", 1)
    build()
    initial_versions, initial_keys = read_state()
    assert len(initial_versions) == 1
    assert initial_keys[first] == initial_keys[third]

    second = insert_purchase(1, "Campinas", 2)
    build()
    versions, keys = read_state()
    assert [(city, start, end) for city, start, end, _ in versions] == [
        ("Sao Paulo", purchased, purchased + timedelta(days=1)),
        ("Campinas", purchased + timedelta(days=1), purchased + timedelta(days=2)),
        ("Sao Paulo", purchased + timedelta(days=2), None),
    ]
    assert keys == {first: versions[0][3], second: versions[1][3], third: versions[2][3]}
    assert keys[first] == initial_keys[first]
    assert keys[third] != initial_keys[third]

    build("--full-refresh")
    assert read_state() == (versions, keys)
