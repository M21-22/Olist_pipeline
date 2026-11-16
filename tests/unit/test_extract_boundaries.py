"""Boundary regression for real Parquet extraction, without PostgreSQL or S3."""
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import pyarrow.parquet as pq

from include import extract
from include.common import TABLE_COLUMNS

UTC = timezone.utc
END = datetime(2026, 11, 16, 12, 15, tzinfo=UTC)
LOWER = END - timedelta(minutes=25)  # 15-minute interval + 10-minute overlap


class FakeCursor:
    def __init__(self, rows=None):
        self.rows = list(rows or [])
        self.sql = None
        self.params = None
        self.itersize = None

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def execute(self, sql, params=None):
        self.sql, self.params = sql, params

    def fetchmany(self, count):
        rows, self.rows = self.rows[:count], self.rows[count:]
        return rows


class FakeConnection:
    def __init__(self, source_cursor):
        self.control = FakeCursor()
        self.source = source_cursor

    def cursor(self, name=None):
        return self.source if name else self.control

    def rollback(self):
        pass

    def close(self):
        pass


class FakeHook:
    def __init__(self, connection):
        self.connection = connection

    def get_conn(self):
        return self.connection


def order_row(key, timestamp):
    return tuple(
        key if col == "order_id" else timestamp if col == "updated_at" else None
        for col, _ in TABLE_COLUMNS["orders"]
    )


def test_extract_interval_uses_exclusive_lower_and_inclusive_upper(tmp_path: Path):
    # Mimics the SQL predicate applied by PostgreSQL; also assert the emitted SQL.
    events = [
        ("exact_lower", LOWER),
        ("inside_lower", LOWER + timedelta(microseconds=1)),
        ("logical_start", END - timedelta(minutes=15)),
        ("exact_upper", END),
        ("after_upper", END + timedelta(microseconds=1)),
    ]
    selected = [order_row(key, dt) for key, dt in events if LOWER < dt <= END]
    source = FakeCursor(selected)
    path = tmp_path / "boundary.parquet"
    with patch.object(extract, "PostgresHook", return_value=FakeHook(FakeConnection(source))):
        stats = extract._write_parquet("orders", LOWER, END, path)

    assert "updated_at > %s AND updated_at <= %s" in source.sql
    assert source.params == (LOWER, END)
    assert stats["row_count"] == 3
    assert pq.read_table(path, columns=["order_id"]).column("order_id").to_pylist() == [
        "inside_lower", "logical_start", "exact_upper"
    ]


def test_extraction_preserves_schema_for_zero_rows(tmp_path: Path):
    source = FakeCursor()
    path = tmp_path / "empty.parquet"
    with patch.object(extract, "PostgresHook", return_value=FakeHook(FakeConnection(source))):
        stats = extract._write_parquet("orders", LOWER, END, path)
    assert pq.ParquetFile(path).metadata.num_rows == 0
    assert stats["row_count"] == 0
    assert stats["min_updated_at"] is None
    assert stats["max_updated_at"] is None
    assert all(v == 0 for v in stats["null_counts"].values())
