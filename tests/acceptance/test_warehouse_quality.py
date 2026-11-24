"""Warehouse quality checks against disposable PostgreSQL, Airflow, dbt, Soda and S3."""

import hashlib
import io
import json
import os
import sqlite3
import subprocess
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from datetime import UTC, datetime, timedelta
from pathlib import Path
from time import monotonic, sleep
from types import SimpleNamespace

import psycopg
import pyarrow.parquet as pq
import pytest
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from include import reconcile, transform

pytestmark = pytest.mark.skipif(
    os.environ.get("OLIST_TEST_WAREHOUSE_QUALITY") != "1", reason="Run with make test-warehouse-quality"
)
HOST = "test-db"
SQL_ROOT = Path("/opt/airflow/infra/postgres/warehouse-init")
SOURCE_SCHEMA = Path("/opt/airflow/emulator/sql/001_schema.sql")


@pytest.fixture(scope="module")
def warehouse_stack(tmp_path_factory, request):
    tmp_path = tmp_path_factory.mktemp("warehouse-quality")
    monkeypatch = pytest.MonkeyPatch()
    request.addfinalizer(monkeypatch.undo)

    def admin(database, *, autocommit=False):
        return psycopg.connect(
            host=HOST, dbname=database, user="postgres",
            password="ci_postgres_only", autocommit=autocommit,
        )

    def fetch(database, sql, params=None):
        with admin(database) as conn:
            return conn.execute(sql, params).fetchall()

    def execute(database, sql, params=None):
        with admin(database) as conn:
            conn.execute(sql, params)


    # This process creates its own source and warehouse databases in the
    # disposable Compose project, using its test-only administrator.
    with admin("warehouse_test", autocommit=True) as conn:
        conn.execute("CREATE DATABASE source")
        conn.execute("CREATE DATABASE warehouse")
        conn.execute("CREATE ROLE source_reader LOGIN PASSWORD 'ci_source_only'")
        conn.execute("ALTER ROLE ingest_audit_writer LOGIN PASSWORD 'ci_audit_only'")
        conn.execute("ALTER ROLE soda_reader LOGIN PASSWORD 'ci_soda_only'")

    checkpoint = datetime.now(UTC).replace(second=0, microsecond=0)
    checkpoint -= timedelta(minutes=checkpoint.minute % 15 + 15)
    timestamp = checkpoint - timedelta(minutes=12)
    with admin("source") as conn:
        conn.execute("CREATE SCHEMA shop; CREATE SCHEMA chaos")
        conn.execute(SOURCE_SCHEMA.read_text())
        conn.execute("GRANT USAGE ON SCHEMA shop TO source_reader")
        conn.execute("GRANT SELECT ON ALL TABLES IN SCHEMA shop TO source_reader")
        conn.execute("SET LOCAL olist.skip_touch = 'on'")
        conn.execute(
            """INSERT INTO shop.customers VALUES
               ('d12-customer', 'd12-person', '12345', 'Sao Paulo', 'SP', %s),
               ('d13-customer', 'd13-person', '23456', 'Recife', 'PE', %s)""",
            (timestamp, timestamp),
        )
        conn.execute(
            """INSERT INTO shop.orders
               (order_id, customer_id, order_status, order_purchase_timestamp, updated_at)
               VALUES ('d12-order', 'd12-customer', '  CrEaTeD ', %s, %s),
                      ('d09-order', 'd12-customer', 'invented', %s, %s)""",
            (timestamp, timestamp, timestamp, timestamp),
        )
        conn.execute("INSERT INTO shop.products (product_id, updated_at) VALUES ('fixture-product', %s)", (timestamp,))
        conn.execute(
            """INSERT INTO shop.sellers (seller_id, seller_zip_code_prefix, seller_city, seller_state, updated_at)
               VALUES ('fixture-seller', '12345', 'Sao Paulo', 'SP', %s)""", (timestamp,),
        )
        conn.execute(
            """INSERT INTO shop.order_items
                   (item_pk, order_id, order_item_id, product_id, seller_id, price, freight_value, updated_at)
               VALUES (1, 'd12-order', 1, 'fixture-product', 'fixture-seller', 20, 5, %s),
                      (2, 'd12-order', 2, 'fixture-product', 'fixture-seller', -10, 0, %s)""",
            (timestamp, timestamp),
        )
        conn.execute(
            """INSERT INTO shop.order_payments
                   (order_id, payment_sequential, payment_type, payment_installments, payment_value, updated_at)
               VALUES ('d12-order', 1, 'credit_card', 1, 25, %s)""", (timestamp,),
        )
        conn.execute(
            """INSERT INTO shop.order_reviews (review_pk, order_id, review_score, review_creation_date, updated_at)
               VALUES (1, 'd12-order', 7, %s, %s)""", (timestamp, timestamp),
        )
        conn.execute(
            """INSERT INTO shop.orders
                   (order_id, customer_id, order_status, order_purchase_timestamp, order_approved_at, updated_at)
               VALUES ('d08-order', 'd12-customer', 'approved', %s, %s, %s)""",
            (timestamp, timestamp - timedelta(minutes=1), timestamp),
        )
    with admin("warehouse") as conn:
        for path in sorted(SQL_ROOT.glob("*.sql")):
            if path.name.startswith("01-"):
                continue
            conn.execute(path.read_text().replace(':"DBNAME"', '"warehouse"'))

    conn_vars = {
        "OLIST_SOURCE": ("source_reader", "ci_source_only", "source"),
        "OLIST_AUDIT": ("ingest_audit_writer", "ci_audit_only", "warehouse"),
        "OLIST_LOADER": ("etl_loader", "ci_loader_only", "warehouse"),
        "OLIST_RECONCILE": ("dbt_runner", "ci_dbt_only", "warehouse"),
    }
    for name, (role, password, database) in conn_vars.items():
        monkeypatch.setenv(f"AIRFLOW_CONN_{name}", f"postgresql://{role}:{password}@{HOST}:5432/{database}")
    monkeypatch.setenv("DBT_HOST", HOST)
    monkeypatch.setenv("DBT_RUNNER_PASSWORD", "ci_dbt_only")
    monkeypatch.setenv("SODA_READER_PASSWORD", "ci_soda_only")
    monkeypatch.setenv("AIRFLOW_HOME", str(tmp_path))
    monkeypatch.setenv("AIRFLOW__CORE__LOAD_EXAMPLES", "false")
    monkeypatch.setenv("AIRFLOW__CORE__DAGS_FOLDER", "/opt/airflow/dags")
    monkeypatch.setenv("AIRFLOW__SCHEDULER__CREATE_CRON_DATA_INTERVALS", "false")
    metadata = tmp_path / "airflow.db"
    monkeypatch.setenv("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN", f"sqlite:///{metadata}")

    def airflow(*args):
        result = subprocess.run(["airflow", *args], capture_output=True, text=True, timeout=180, check=False)
        output = result.stdout + result.stderr
        assert result.returncode == 0, output
        return output

    def run_dag(name, minute):
        logical_date = checkpoint + timedelta(minutes=minute)
        output = airflow("dags", "test", name, logical_date.isoformat())
        with closing(sqlite3.connect(metadata)) as conn:
            state = conn.execute(
                "SELECT state FROM dag_run WHERE dag_id = ? ORDER BY id DESC LIMIT 1", (name,)
            ).fetchone()
        assert state == ("success",), output

    def run_dbt(*arguments, check_status=None):
        target = tmp_path / "dbt"
        result = subprocess.run(
            [transform.DBT, *arguments, "--project-dir", transform.PROJECT,
             "--profiles-dir", transform.PROJECT, "--target-path", str(target),
             "--log-path", str(tmp_path / "dbt-logs")],
            capture_output=True, text=True, timeout=120, check=False,
        )
        assert result.returncode == (1 if check_status == "fail" else 0), result.stdout + result.stderr
        if check_status:
            [entry] = json.loads((target / "run_results.json").read_text())["results"]
            assert entry["status"] == check_status and entry["failures"] > 0, entry

    s3 = S3Hook(aws_conn_id="olist_s3").get_conn()
    s3.create_bucket(Bucket="landing")
    airflow("db", "migrate")
    run_dag("ingest", 0)
    checkpoints = fetch(
        "warehouse",
        "SELECT table_name, certified_high_watermark, last_certified_batch_id "
        "FROM audit.certified_watermarks ORDER BY table_name",
    )
    assert len(checkpoints) == 7
    assert all(point == checkpoint for _, point, _ in checkpoints)

    run_dag("reconcile", 5)
    run_dag("transform_marts", 1)
    assert fetch("warehouse", "SELECT status FROM audit.serving_state") == [("HEALTHY",)]

    def read_object(key):
        with closing(s3.get_object(Bucket="landing", Key=key)["Body"]) as body:
            return body.read()

    return SimpleNamespace(
        admin=admin, fetch=fetch, execute=execute, run_dag=run_dag, run_dbt=run_dbt,
        timestamp=timestamp, checkpoint=checkpoint, checkpoints=checkpoints, read_object=read_object,
        tmp_path=tmp_path,
    )


def test_d12_silent_customer_update_reaches_staging(warehouse_stack):
    timestamp = warehouse_stack.timestamp
    fetch = warehouse_stack.fetch
    with warehouse_stack.admin("source") as conn:
        conn.execute("SET LOCAL olist.skip_touch = 'on'")
        updated = conn.execute(
            """UPDATE shop.customers SET customer_city = 'rio de janeiro'
               WHERE customer_id = 'd12-customer' RETURNING updated_at"""
        ).fetchall()
        assert updated == [(timestamp,)]

    warehouse_stack.run_dag("reconcile", 5)
    [(status, mismatches, unrepaired, details)] = fetch(
        "warehouse",
        """SELECT status, mismatch_count, unrepaired_count, details
           FROM audit.reconciliation WHERE table_name = 'customers'
           ORDER BY reconciliation_id DESC LIMIT 1""",
    )
    assert (status, mismatches, unrepaired) == ("MATCH", 1, 0)
    assert details["changed_count"] == 1
    assert details["changed"] == ["d12-customer"]
    batch_id = details["repair_batch_id"]
    [(parquet_key, manifest_key)] = fetch(
        "warehouse",
        """SELECT landed.parquet_key, landed.manifest_key
           FROM audit.landed_batches AS landed JOIN audit.batch_registry AS registry USING (batch_id)
           WHERE landed.batch_id = %s AND landed.extraction_mode = 'repair'
             AND registry.status = 'CERTIFIED'""",
        (batch_id,),
    )
    gates = fetch("warehouse", "SELECT gate, status FROM audit.dq_results WHERE batch_id = %s", (batch_id,))
    assert set(gates) == {("GATE_1", "PASS"), ("GATE_2", "PASS")}
    assert fetch(
        "warehouse",
        "SELECT table_name, certified_high_watermark, last_certified_batch_id "
        "FROM audit.certified_watermarks ORDER BY table_name",
    ) == warehouse_stack.checkpoints

    manifest = json.loads(warehouse_stack.read_object(manifest_key))
    parquet = warehouse_stack.read_object(parquet_key)
    assert manifest["batch_id"] == batch_id and manifest["mode"] == "repair"
    assert hashlib.sha256(parquet).hexdigest() == manifest["sha256"]
    assert pq.read_table(io.BytesIO(parquet), columns=["customer_id", "customer_city", "updated_at"]).to_pylist() == [
        {"customer_id": "d12-customer", "customer_city": "rio de janeiro", "updated_at": timestamp}
    ]

    warehouse_stack.run_dag("transform_marts", 6)
    assert fetch(
        "warehouse", "SELECT customer_city, updated_at, source_batch_id FROM staging.stg_customers WHERE customer_id = 'd12-customer'"
    ) == [("Rio De Janeiro", timestamp, batch_id)]
    assert fetch("warehouse", "SELECT status FROM audit.serving_state") == [("HEALTHY",)]


@pytest.mark.parametrize(
    ("table", "key", "reason", "fact", "fact_key"),
    [
        ("order_items", "2", "negative_price", "fct_order_items", "item_pk::text"),
        ("order_reviews", "1", "invalid_review_score", "fct_reviews", "review_pk::text"),
        ("orders", "d08-order", "approved_before_purchase", "fct_orders", "order_id"),
        ("orders", "d09-order", "unknown_order_status", "fct_orders", "order_id"),
    ],
    ids=["D08-negative-price", "D08-review-score", "D08-timestamp-order", "D09-unknown-status"],
)
def test_invalid_source_rows_are_quarantined(warehouse_stack, table, key, reason, fact, fact_key):
    assert warehouse_stack.fetch(
        "warehouse",
        """SELECT reason_code FROM quarantine.rejected_rows
           WHERE source_table = %s AND business_key = %s""",
        (table, key),
    ) == [(reason,)]
    assert warehouse_stack.fetch("warehouse", f"SELECT 1 FROM marts.{fact} WHERE {fact_key} = %s", (key,)) == []


def test_d10_orphan_warns_and_uses_unknown_dimension(warehouse_stack):
    warehouse_stack.execute("warehouse", "UPDATE staging.stg_order_items SET product_id = 'absent' WHERE item_pk = 1")
    try:
        warehouse_stack.run_dbt(
            "test", "--select", "stg_order_items,test_name:relationships",
        )
        artifact = warehouse_stack.tmp_path / "dbt" / "run_results.json"
        warnings = [entry for entry in json.loads(artifact.read_text())["results"] if entry["status"] == "warn"]
        assert len(warnings) == 1 and "product_id" in warnings[0]["unique_id"], warnings
        warehouse_stack.run_dbt("run", "--select", "fct_order_items")
        assert warehouse_stack.fetch(
            "warehouse",
            "SELECT product_sk = md5('__olist_unknown_product__') FROM marts.fct_order_items WHERE item_pk = 1",
        ) == [(True,)]
    finally:
        warehouse_stack.execute("warehouse", "UPDATE staging.stg_order_items SET product_id = 'fixture-product' WHERE item_pk = 1")
        warehouse_stack.run_dbt("run", "--select", "fct_order_items")


def test_d11_late_certification_does_not_replace_newer_order(warehouse_stack):
    with warehouse_stack.admin("warehouse") as conn:
        conn.execute(
            """INSERT INTO audit.landed_batches
               (batch_id, table_name, extraction_mode, logical_interval_end, parquet_key, manifest_key, row_count)
               VALUES ('d11-late', 'orders', 'incremental', %s, 'fixture.parquet', 'fixture.json', 1)""",
            (warehouse_stack.checkpoint - timedelta(minutes=1),),
        )
        conn.execute(
            """INSERT INTO audit.batch_registry (batch_id, table_name, status, expected_rows, certified_at)
               VALUES ('d11-late', 'orders', 'CERTIFIED', 1, clock_timestamp())"""
        )
        conn.execute(
            """INSERT INTO raw.orders
               (order_id, customer_id, order_status, order_purchase_timestamp, updated_at,
                _batch_id, _source_file, _row_hash)
               VALUES ('d12-order', 'd12-customer', 'approved', %s, %s,
                       'd11-late', 'fixture.parquet', 'fixture')""",
            (warehouse_stack.timestamp, warehouse_stack.timestamp - timedelta(minutes=1)),
        )
    warehouse_stack.run_dbt("run", "--select", "stg_orders")
    assert warehouse_stack.fetch(
        "warehouse", "SELECT order_status, updated_at FROM staging.stg_orders WHERE order_id = 'd12-order'"
    ) == [("created", warehouse_stack.timestamp)]
    assert warehouse_stack.fetch(
        "warehouse", "SELECT EXISTS (SELECT 1 FROM audit.dbt_staging_processed_batches WHERE batch_id = 'd11-late')"
    ) == [(True,)]


def test_d13_hard_delete_and_reinsertion(warehouse_stack):
    warehouse_stack.execute("source", "DELETE FROM shop.customers WHERE customer_id = 'd13-customer'")
    warehouse_stack.run_dag("reconcile", 5)
    [(details, unrepaired)] = warehouse_stack.fetch(
        "warehouse",
        """SELECT details, unrepaired_count FROM audit.reconciliation
           WHERE table_name = 'customers' ORDER BY reconciliation_id DESC LIMIT 1""",
    )
    assert unrepaired == 0 and details["deleted_count"] == 1
    assert warehouse_stack.fetch(
        "warehouse", "SELECT business_key FROM audit.deleted_keys WHERE source_table = 'customers'"
    ) == [("d13-customer",)]
    [(manifest_key,)] = warehouse_stack.fetch(
        "warehouse", "SELECT manifest_key FROM audit.landed_batches WHERE batch_id = %s",
        (details["repair_batch_id"],),
    )
    manifest = json.loads(warehouse_stack.read_object(manifest_key))
    assert manifest["row_count"] == 0 and manifest["deleted_keys"] == ["d13-customer"]
    warehouse_stack.run_dag("transform_marts", 7)
    assert warehouse_stack.fetch(
        "warehouse", "SELECT 1 FROM staging.stg_customers WHERE customer_id = 'd13-customer'"
    ) == []

    with warehouse_stack.admin("source") as conn:
        conn.execute("SET LOCAL olist.skip_touch = 'on'")
        conn.execute(
            "INSERT INTO shop.customers VALUES ('d13-customer', 'd13-person', '23456', 'Recife', 'PE', %s)",
            (warehouse_stack.timestamp,),
        )
    warehouse_stack.run_dag("reconcile", 5)
    assert warehouse_stack.fetch(
        "warehouse", "SELECT 1 FROM audit.deleted_keys WHERE business_key = 'd13-customer'"
    ) == []
    warehouse_stack.run_dag("transform_marts", 8)
    assert warehouse_stack.fetch(
        "warehouse", "SELECT customer_city, updated_at FROM staging.stg_customers WHERE customer_id = 'd13-customer'"
    ) == [("Recife", warehouse_stack.timestamp)]
    assert warehouse_stack.fetch("warehouse", "SELECT status FROM audit.serving_state") == [("HEALTHY",)]
    assert warehouse_stack.fetch(
        "warehouse",
        "SELECT table_name, certified_high_watermark, last_certified_batch_id "
        "FROM audit.certified_watermarks ORDER BY table_name",
    ) == warehouse_stack.checkpoints

    assert warehouse_stack.fetch(
        "warehouse",
        "SELECT table_name, landed_high_watermark, last_landed_batch_id "
        "FROM audit.extraction_watermarks ORDER BY table_name",
    ) == warehouse_stack.checkpoints


def assert_soda_outcome(warehouse_stack, case, check, expected):
    output = warehouse_stack.tmp_path / case
    output.mkdir(exist_ok=True)
    conn = PostgresHook(postgres_conn_id="olist_reconcile").get_conn()
    with closing(conn):
        if expected == "FAIL":
            with pytest.raises(RuntimeError, match=r"Soda Gate 4 scan failed \(exit 2\)"):
                transform._soda_step(conn, case, output)
        else:
            assert transform._soda_step(conn, case, output) is (expected == "WARN")
    assert warehouse_stack.fetch(
        "warehouse", "SELECT status FROM audit.dq_results WHERE batch_id = %s AND check_name LIKE %s",
        (case, f"soda/{check} [%"),
    ) == [(expected,)]


def test_soda_checks_latest_reconciliation_and_coverage(warehouse_stack):
    coverage = "Missing or stale reconciliation evidence"
    drift = "Latest reconciliation has unresolved drift"
    table = "reconciliation_coverage_fixture"
    warehouse_stack.execute(
        "warehouse",
        "UPDATE audit.reconciliation SET table_name = %s WHERE table_name = 'products'",
        (table,),
    )
    try:
        assert_soda_outcome(warehouse_stack, "missing-reconciliation", coverage, "FAIL")
        warehouse_stack.execute(
            "warehouse",
            """INSERT INTO audit.reconciliation (table_name, status, unrepaired_count, checked_at)
               VALUES ('products', 'MATCH', 0, now() - interval '31 hours')""",
        )
        assert_soda_outcome(warehouse_stack, "stale-reconciliation", coverage, "FAIL")
        warehouse_stack.execute(
            "warehouse",
            """INSERT INTO audit.reconciliation (table_name, status, unrepaired_count)
               VALUES ('products', 'MISMATCH', 1)""",
        )
        assert_soda_outcome(warehouse_stack, "current-mismatch", drift, "FAIL")
        warehouse_stack.execute(
            "warehouse",
            """INSERT INTO audit.reconciliation (table_name, status, unrepaired_count, details)
               VALUES ('products', 'DEFERRED', 0, '{"deferred_count": 2}')""",
        )
        assert_soda_outcome(warehouse_stack, "match-with-deferrals", drift, "PASS")
    finally:
        warehouse_stack.execute("warehouse", "DELETE FROM audit.reconciliation WHERE table_name = 'products'")
        warehouse_stack.execute(
            "warehouse",
            "UPDATE audit.reconciliation SET table_name = 'products' WHERE table_name = %s",
            (table,),
        )


def test_d14_missing_raw_row_blocks_until_reconciled(warehouse_stack):
    warehouse_stack.execute("warehouse", "DELETE FROM raw.products WHERE product_id = 'fixture-product'")
    with pytest.raises(reconcile.ReconciliationMismatch, match="missing=1"):
        reconcile.compare_table("products")
    assert_soda_outcome(warehouse_stack, "D14-before", "Latest reconciliation has unresolved drift", "FAIL")
    warehouse_stack.run_dag("reconcile", 5)
    [(details, status, unrepaired)] = warehouse_stack.fetch(
        "warehouse",
        """SELECT details, status, unrepaired_count FROM audit.reconciliation
           WHERE table_name = 'products' ORDER BY reconciliation_id DESC LIMIT 1""",
    )
    assert (status, unrepaired) == ("MATCH", 0)
    assert details["missing_count"] == 1 and details["repair_batch_id"]
    warehouse_stack.run_dag("transform_marts", 9)
    assert warehouse_stack.fetch("warehouse", "SELECT status FROM audit.serving_state") == [("HEALTHY",)]


@pytest.mark.parametrize(("age", "outcome"), [(60, "WARN"), (100, "FAIL")], ids=["D15-warn", "D15-block"])
def test_stale_certified_window(warehouse_stack, age, outcome):
    moved_by = warehouse_stack.checkpoint - (datetime.now(UTC) - timedelta(minutes=age))
    sql = """UPDATE audit.landed_batches SET logical_interval_end = logical_interval_end + %s
             WHERE table_name = 'orders' AND extraction_mode = 'incremental'"""
    warehouse_stack.execute("warehouse", sql, (-moved_by,))
    try:
        warehouse_stack.run_dbt("run", "--select", "gate4_metrics")
        assert_soda_outcome(warehouse_stack, f"D15-{age}", "Certified input freshness", outcome)
    finally:
        warehouse_stack.execute("warehouse", sql, (moved_by,))
        warehouse_stack.run_dbt("run", "--select", "gate4_metrics")


@pytest.mark.parametrize(
    ("case", "metric", "table", "baseline", "anomaly"),
    [
        ("D16", "mart_rows", "fct_orders", 1, 10),
        ("D17", "mean_payment", "fct_payments", 25, 80),
    ],
    ids=["D16-volume", "D17-payment-distribution"],
)
def test_soda_history_detects_drift(warehouse_stack, case, metric, table, baseline, anomaly):
    bounds = (metric, table)
    [(original,)] = warehouse_stack.fetch(
        "warehouse", "SELECT metric_value FROM marts.gate4_metrics WHERE metric_name = %s AND object_name = %s", bounds,
    )
    sql = "UPDATE marts.gate4_metrics SET metric_value = %s WHERE metric_name = %s AND object_name = %s"
    with warehouse_stack.admin("warehouse") as conn:
        conn.execute(
            """INSERT INTO audit.metrics_history (metric_name, object_name, measured_at, metric_value)
               SELECT %s, %s, CURRENT_TIMESTAMP + n * interval '1 microsecond', %s
               FROM generate_series(1, 7) AS n""",
            (*bounds, baseline),
        )
        conn.execute(sql, (anomaly, *bounds))
    try:
        assert_soda_outcome(warehouse_stack, case, "Volume and distribution drift against accepted transform history", "WARN")
    finally:
        warehouse_stack.execute("warehouse", sql, (original, *bounds))


def test_calibrated_soda_threshold_uses_distinct_prior_windows(warehouse_stack):
    series = ("settlement_difference_rate", "fct_orders")
    check = "Volume and distribution drift against accepted transform history"
    window_end = warehouse_stack.checkpoint
    clean_from = window_end - timedelta(hours=9)
    clean_until = window_end
    update_metric = """UPDATE marts.gate4_metrics SET metric_value = %s
                       WHERE metric_name = %s AND object_name = %s"""
    [(original,)] = warehouse_stack.fetch(
        "warehouse", "SELECT metric_value FROM marts.gate4_metrics WHERE metric_name = %s AND object_name = %s",
        series,
    )
    with warehouse_stack.admin("warehouse") as conn:
        conn.execute(
            """INSERT INTO audit.metrics_history
                   (metric_name, object_name, measured_at, source_interval_end, metric_value, baseline_eligible)
               SELECT %s, %s, window_end + retry.delay, window_end,
                      CASE WHEN retry.delay = interval '0 second' THEN 0.005 ELSE 0.9 END, true
               FROM generate_series(%s::timestamptz, %s::timestamptz, interval '15 minutes') AS window_end
               CROSS JOIN (VALUES (interval '0 second'), (interval '1 second')) AS retry(delay)""",
            (*series, window_end - timedelta(hours=7), window_end - timedelta(minutes=15)),
        )
        conn.execute(update_metric, (0.01, *series))
    try:
        assert_soda_outcome(warehouse_stack, "calibration-default", check, "PASS")
        [profile] = warehouse_stack.fetch(
            "warehouse",
            """SELECT fit_samples, validation_samples, absolute_floor, calibrated_validation_warnings
               FROM audit.profile_soda_drift(%s, %s) WHERE (metric_name, object_name) = (%s, %s)""",
            (clean_from, clean_until, *series),
        )
        assert profile == (14, 7, 0.001, 0)
        [insufficient] = warehouse_stack.fetch(
            "warehouse",
            """SELECT fit_samples FROM audit.profile_soda_drift(%s, %s)
               WHERE (metric_name, object_name) = (%s, %s)""",
            (window_end - timedelta(hours=2), clean_until, *series),
        )
        assert insufficient == (0,)
        warehouse_stack.execute(
            "warehouse",
            """INSERT INTO audit.soda_drift_calibration
                    (metric_name, object_name, absolute_floor, fit_samples, validation_samples,
                     default_validation_warnings, calibrated_validation_warnings,
                     first_source_interval_end, last_source_interval_end)
               SELECT metric_name, object_name, absolute_floor, fit_samples, validation_samples,
                      default_validation_warnings, calibrated_validation_warnings,
                      first_source_interval_end, last_source_interval_end
               FROM audit.profile_soda_drift(%s, %s) WHERE (metric_name, object_name) = (%s, %s)""",
            (clean_from, clean_until, *series),
        )
        assert_soda_outcome(warehouse_stack, "calibration-applied", check, "WARN")
    finally:
        warehouse_stack.execute("warehouse", update_metric, (original, *series))
        warehouse_stack.execute(
            "warehouse", "DELETE FROM audit.soda_drift_calibration WHERE (metric_name, object_name) = (%s, %s)",
            series,
        )
        warehouse_stack.execute(
            "warehouse",
            """DELETE FROM audit.metrics_history
               WHERE (metric_name, object_name) = (%s, %s) AND measured_at >= %s AND measured_at < %s""",
            (*series, clean_from, clean_until),
        )


def test_d18_overlapping_current_customer_versions_fail_dbt(warehouse_stack):
    warehouse_stack.execute(
        "warehouse",
        """INSERT INTO marts.dim_customer
           SELECT 'd18-overlap', customer_unique_id, representative_customer_id, customer_zip_code_prefix,
                  customer_city, customer_state, valid_from + interval '1 second', valid_to, true, false
           FROM marts.dim_customer WHERE customer_unique_id = 'd12-person'""",
    )
    try:
        warehouse_stack.run_dbt("test", "--select", "assert_dim_customer_nonoverlapping_versions", check_status="fail")
    finally:
        warehouse_stack.execute("warehouse", "DELETE FROM marts.dim_customer WHERE customer_sk = 'd18-overlap'")


def test_d19_text_noise_is_normalized(warehouse_stack):
    assert warehouse_stack.fetch(
        "source", "SELECT order_status FROM shop.orders WHERE order_id = 'd12-order'"
    ) == [("  CrEaTeD ",)]
    assert warehouse_stack.fetch(
        "warehouse", "SELECT order_status, is_valid FROM staging.stg_orders WHERE order_id = 'd12-order'"
    ) == [("created", True)]


def test_d20_sensitive_api_view_column_fails_dbt(warehouse_stack):
    [(view_sql,)] = warehouse_stack.fetch("warehouse", "SELECT pg_get_viewdef('api.orders'::regclass)")
    warehouse_stack.execute(
        "warehouse",
        "CREATE OR REPLACE VIEW api.orders AS SELECT original.*, 'fixture@example.test'::text AS email "
        f"FROM ({view_sql.rstrip().removesuffix(';')}) AS original",
    )
    try:
        warehouse_stack.run_dbt("test", "--select", "assert_api_no_sensitive_columns", check_status="fail")
    finally:
        with warehouse_stack.admin("warehouse") as conn:
            conn.execute("DROP VIEW api.orders")
            conn.execute(f"CREATE VIEW api.orders AS {view_sql}")
            conn.execute("ALTER VIEW api.orders OWNER TO dbt_runner")
            conn.execute("GRANT SELECT ON api.orders TO api_reader, grafana_reader")


def test_d21_component_payment_disagreement_blocks_soda(warehouse_stack):
    warehouse_stack.execute("warehouse", "UPDATE marts.fct_payments SET payment_value = 35 WHERE order_id = 'd12-order'")
    try:
        assert_soda_outcome(warehouse_stack, "D21", "Order fact item and payment aggregates agree with component facts", "FAIL")
    finally:
        warehouse_stack.execute("warehouse", "UPDATE marts.fct_payments SET payment_value = 25 WHERE order_id = 'd12-order'")


def test_reconciliation_detects_changed_raw_payload_with_unchanged_hash(warehouse_stack):
    with warehouse_stack.admin("warehouse") as conn:
        (stored_hash,) = conn.execute(
            """UPDATE raw.sellers SET seller_city = 'incorrect'
               WHERE seller_id = 'fixture-seller' RETURNING _row_hash"""
        ).fetchone()
    try:
        with pytest.raises(reconcile.ReconciliationMismatch, match="changed=1"):
            reconcile.compare_table("sellers")
        assert warehouse_stack.fetch(
            "warehouse", "SELECT _row_hash FROM raw.sellers WHERE seller_id = 'fixture-seller'"
        ) == [(stored_hash,)]
    finally:
        warehouse_stack.execute(
            "warehouse", "UPDATE raw.sellers SET seller_city = 'Sao Paulo' WHERE seller_id = 'fixture-seller'"
        )
        reconcile.compare_table("sellers")


def test_soda_reader_is_limited_to_its_inputs(warehouse_stack):
    assert warehouse_stack.fetch(
        "warehouse",
        """SELECT has_schema_privilege('soda_reader', 'raw', 'USAGE'),
                  has_schema_privilege('soda_reader', 'staging', 'USAGE'),
                  has_table_privilege('soda_reader', 'raw.order_reviews', 'SELECT'),
                  has_table_privilege('soda_reader', 'marts.fct_reviews', 'SELECT'),
                  has_table_privilege('soda_reader', 'marts.fct_orders', 'SELECT'),
                  has_table_privilege('soda_reader', 'audit.reconciliation', 'SELECT'),
                  has_table_privilege('soda_reader', 'audit.dq_results', 'SELECT')""",
    ) == [(False, False, False, False, True, True, False)]


def test_staging_uses_only_captured_batch_versions(warehouse_stack):
    order = "phase7-cutoff-order"
    before, after = "phase7-cutoff-before", "phase7-cutoff-after"

    def certify(conn, batch_id, state, offset):
        stamp = warehouse_stack.timestamp + timedelta(seconds=offset)
        conn.execute(
            """INSERT INTO audit.landed_batches
                   (batch_id, table_name, extraction_mode, logical_interval_end, parquet_key, manifest_key, row_count)
               VALUES (%s, 'orders', 'incremental', %s, 'fixture.parquet', 'fixture.json', 1)""",
            (batch_id, stamp),
        )
        conn.execute(
            """INSERT INTO audit.batch_registry (batch_id, table_name, status, expected_rows, certified_at)
               VALUES (%s, 'orders', 'CERTIFIED', 1, %s)""",
            (batch_id, stamp),
        )
        conn.execute(
            """INSERT INTO raw.orders (order_id, order_status, updated_at, _batch_id, _source_file, _row_hash)
               VALUES (%s, %s, %s, %s, 'cutoff-fixture', %s)""",
            (order, state, stamp, batch_id, batch_id),
        )

    with warehouse_stack.admin("warehouse") as conn:
        certify(conn, before, "created", 1)

    with ThreadPoolExecutor(max_workers=1) as executor:
        with warehouse_stack.admin("warehouse") as conn:
            conn.execute("LOCK TABLE audit.deleted_keys IN ACCESS EXCLUSIVE MODE")
            certify(conn, after, "shipped", 2)
            build = executor.submit(warehouse_stack.run_dbt, "run", "--select", "stg_orders")
            deadline = monotonic() + 45
            # The tombstone pre-hook runs after the pending-batches hook.
            while not conn.execute(
                """SELECT EXISTS (
                       SELECT 1 FROM pg_locks AS waiting
                       JOIN pg_stat_activity AS session ON session.pid = waiting.pid
                       WHERE waiting.relation = 'audit.deleted_keys'::regclass
                         AND NOT waiting.granted AND session.usename = 'dbt_runner'
                   )"""
            ).fetchone()[0]:
                conn.execute("SELECT pg_stat_clear_snapshot()")
                if build.done():
                    build.result()
                    pytest.fail("dbt completed without waiting at the tombstone hook")
                if monotonic() >= deadline:
                    pytest.fail("dbt never reached the tombstone hook")
                sleep(0.05)
        build.result()

    query = "SELECT source_batch_id FROM staging.stg_orders WHERE order_id = %s"
    assert warehouse_stack.fetch("warehouse", query, (order,)) == [(before,)]
    warehouse_stack.run_dbt("run", "--select", "stg_orders")
    assert warehouse_stack.fetch("warehouse", query, (order,)) == [(after,)]
