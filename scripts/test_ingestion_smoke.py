"""Non-destructive Olist Gate 1 / Gate 2 smoke tests.

Run inside a Docker Airflow worker with /opt/airflow/include on PYTHONPATH:
    python /opt/airflow/scripts/test_ingestion_smoke.py

Tests use generated Parquet files and fake Gate 2 cursors. No network or database writes.
"""
import csv
import hashlib
import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from include.common import CONTROL_COLUMNS, TABLE_COLUMNS, expected_schema, sha256_file
from include.gates.gate1 import Gate1Failure, validate_landed_batch
from include.gates.gate2 import validate_loaded_batch
from include.load import _write_copy_csv

UTC = timezone.utc
END = datetime(2026, 11, 13, 0, 15, tzinfo=UTC)
START = END - timedelta(minutes=15)


def example_row(table, pk, *, ts=None):
    """Create a schema-correct minimal record; nullable non-key columns stay NULL."""
    row = {name: None for name, _ in TABLE_COLUMNS[table]}
    row["updated_at"] = ts or (END - timedelta(minutes=1))
    if table == "orders":
        row["order_id"] = pk
    elif table == "order_payments":
        row["order_id"], row["payment_sequential"] = pk
        row["payment_value"] = Decimal("3.50")
    else:
        raise ValueError(table)
    return row


def write_case(table, rows, path, *, row_group_size=1):
    schema = expected_schema(table)
    colmap = {name: [r[name] for r in rows] for name in schema.names}
    data = pa.Table.from_pydict(colmap, schema=schema)
    pq.write_table(data, path, row_group_size=row_group_size, compression="zstd")
    times = [r["updated_at"] for r in rows if r["updated_at"] is not None]
    totals = {
        name: str(sum((r[name] or Decimal(0) for r in rows), Decimal(0)))
        for name in CONTROL_COLUMNS.get(table, ())
    }
    manifest = {
        "table": table, "batch_id": "test-batch", "mode": "incremental",
        "interval_start": START.isoformat(), "interval_end": END.isoformat(),
        "overlap_minutes": 10, "sha256": sha256_file(path),
        "row_count": len(rows),
        "schema_fingerprint": hashlib.sha256(schema.serialize().to_pybytes()).hexdigest(),
        "null_counts": {name: sum(r[name] is None for r in rows) for name in schema.names},
        "control_totals": totals,
        "min_updated_at": min(times).isoformat() if times else None,
        "max_updated_at": max(times).isoformat() if times else None,
    }
    return manifest


def gate1(manifest, path, *, table=None, interval_end=None):
    checks = []
    try:
        result = validate_landed_batch(
            manifest=manifest, parquet_path=path, batch_id="test-batch",
            table=table or manifest["table"], mode="incremental",
            interval_end=interval_end or END, check_results=checks,
        )
        return result, checks
    except Gate1Failure:
        return None, checks


class Gate1Tests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "test.parquet"

    def test_valid_orders(self):
        m = write_case("orders", [example_row("orders", "a"), example_row("orders", "b")], self.path)
        parquet, checks = gate1(m, self.path)
        self.assertIsNotNone(parquet, checks)
        self.assertTrue(checks)
        self.assertTrue(all(x["status"] == "PASS" for x in checks), checks)
        self.assertIn("duckdb_unique_keys", [x["check_name"] for x in checks])
        self.assertIn("gx_updated_at_window", [x["check_name"] for x in checks])

    def test_timezone_equal_instant(self):
        m = write_case("orders", [example_row("orders", "a")], self.path)
        plus_four = timezone(timedelta(hours=4))
        parquet, checks = gate1(m, self.path, interval_end=END.astimezone(plus_four))
        self.assertIsNotNone(parquet, checks)

    def test_cross_row_group_duplicate(self):
        rows = [example_row("orders", "a"), example_row("orders", "b"), example_row("orders", "a")]
        m = write_case("orders", rows, self.path, row_group_size=1)
        parquet, checks = gate1(m, self.path)
        self.assertIsNone(parquet)
        self.assertIn("duckdb_unique_keys", [x["check_name"] for x in checks if x["status"] == "FAIL"])

    def test_composite_key_duplicate(self):
        rows = [example_row("order_payments", ("a", 1)), example_row("order_payments", ("a", 2)), example_row("order_payments", ("a", 1))]
        m = write_case("order_payments", rows, self.path, row_group_size=1)
        parquet, checks = gate1(m, self.path)
        self.assertIsNone(parquet)
        self.assertIn("duckdb_unique_keys", [x["check_name"] for x in checks if x["status"] == "FAIL"])

    def test_window_violation(self):
        m = write_case("orders", [example_row("orders", "a", ts=END + timedelta(seconds=1))], self.path)
        parquet, checks = gate1(m, self.path)
        self.assertIsNone(parquet)
        self.assertIn("gx_updated_at_window", [x["check_name"] for x in checks if x["status"] == "FAIL"])

    def test_wrong_checksum(self):
        m = write_case("orders", [example_row("orders", "a")], self.path)
        m["sha256"] = "0" * 64
        parquet, checks = gate1(m, self.path)
        self.assertIsNone(parquet)
        self.assertIn("sha256", [x["check_name"] for x in checks if x["status"] == "FAIL"])

    def test_manifest_wrong_total(self):
        m = write_case("order_payments", [example_row("order_payments", ("a", 1))], self.path)
        m["control_totals"]["payment_value"] = "999.00"
        parquet, checks = gate1(m, self.path)
        self.assertIsNone(parquet)
        self.assertIn("control_totals", [x["check_name"] for x in checks if x["status"] == "FAIL"])

    def test_missing_source_column(self):
        m = write_case("orders", [example_row("orders", "a")], self.path)
        table = pq.read_table(self.path).drop(["order_status"])
        pq.write_table(table, self.path)
        m["sha256"] = sha256_file(self.path)
        m["schema_fingerprint"] = hashlib.sha256(table.schema.serialize().to_pybytes()).hexdigest()
        parquet, checks = gate1(m, self.path)
        self.assertIsNone(parquet)
        self.assertIn("source_schema", [x["check_name"] for x in checks if x["status"] == "FAIL"])

    def test_empty_interval(self):
        m = write_case("orders", [], self.path)
        parquet, checks = gate1(m, self.path)
        self.assertIsNotNone(parquet, checks)


class FakeGate2Cursor:
    def __init__(self, *, count, sums):
        self.count = count
        self.sums = sums
        self.sql = ""

    def execute(self, query, parameters):
        self.sql = query

    def fetchone(self):
        if "COUNT(*)" in self.sql:
            return (self.count,)
        for name, value in self.sums.items():
            if '"' + name + '"' in self.sql:
                return (value,)
        raise AssertionError("Unexpected SQL: " + self.sql)


class Gate2AndCopyTests(unittest.TestCase):
    def test_gate2_pass(self):
        cursor = FakeGate2Cursor(count=2, sums={"payment_value": Decimal("7.00")})
        validate_loaded_batch(cursor=cursor, table="order_payments", batch_id="b", expected_rows=2, expected_totals={"payment_value": "7.00"})

    def test_gate2_row_count_fail(self):
        cursor = FakeGate2Cursor(count=1, sums={"payment_value": Decimal("7.00")})
        with self.assertRaisesRegex(ValueError, "expected 2 rows"):
            validate_loaded_batch(cursor=cursor, table="order_payments", batch_id="b", expected_rows=2, expected_totals={"payment_value": "7.00"})

    def test_gate2_total_fail(self):
        cursor = FakeGate2Cursor(count=2, sums={"payment_value": Decimal("5.00")})
        with self.assertRaisesRegex(ValueError, "control-total mismatch"):
            validate_loaded_batch(cursor=cursor, table="order_payments", batch_id="b", expected_rows=2, expected_totals={"payment_value": "7.00"})

    def test_copy_csv_null_vs_empty(self):
        with tempfile.TemporaryDirectory() as tmp:
            parquet_path = Path(tmp) / "file.parquet"
            csv_path = Path(tmp) / "file.csv"
            record = example_row("orders", "a")
            record["order_status"] = ""
            manifest = write_case("orders", [record], parquet_path)
            _write_copy_csv(pq.ParquetFile(parquet_path), manifest["batch_id"], "orders/test.parquet", csv_path)
            content = csv_path.read_text()
            self.assertIn('""', content)  # empty TEXT must be quoted
            # NULL columns are unquoted empty values; test by CSV parsing
            with csv_path.open(newline="") as f:
                values = next(csv.reader(f))
            self.assertEqual(values[2], "")
            self.assertEqual(values[3], "")


if __name__ == "__main__":
    unittest.main(verbosity=2)
