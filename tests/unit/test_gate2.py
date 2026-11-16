"""Focused Gate 2 unit tests; run in an Airflow environment with pytest installed.

Requires the Package B.2 code changes:
- Gate2Failure raised by validate_loaded_batch for contract mismatches
- _load_transaction records Gate2Failure after rolling back and closing its connection
"""

from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from include.gates.gate2 import Gate2Failure, validate_loaded_batch
from include import load


@pytest.fixture
def cursor():
    return MagicMock()


def test_gate2_passes_when_rows_and_totals_match(cursor):
    cursor.fetchone.side_effect = [(2,), (Decimal("25.50"),)]

    validate_loaded_batch(
        cursor=cursor,
        table="orders",
        batch_id="batch-1",
        expected_rows=2,
        expected_totals={"payment_value": "25.50"},
    )

    assert cursor.execute.call_count == 2


def test_gate2_rejects_row_count_mismatch(cursor):
    cursor.fetchone.return_value = (1,)

    with pytest.raises(Gate2Failure, match="expected 2 rows, found 1"):
        validate_loaded_batch(
            cursor=cursor,
            table="orders",
            batch_id="batch-1",
            expected_rows=2,
            expected_totals={},
        )


def test_gate2_rejects_control_total_mismatch(cursor):
    cursor.fetchone.side_effect = [(2,), (Decimal("12.00"),)]

    with pytest.raises(Gate2Failure, match="control-total mismatch: price"):
        validate_loaded_batch(
            cursor=cursor,
            table="order_items",
            batch_id="batch-1",
            expected_rows=2,
            expected_totals={"price": "20.00"},
        )


def test_gate2_propagates_database_errors(cursor):
    cursor.execute.side_effect = RuntimeError("database unavailable")

    with pytest.raises(RuntimeError, match="database unavailable"):
        validate_loaded_batch(
            cursor=cursor,
            table="orders",
            batch_id="batch-1",
            expected_rows=0,
            expected_totals={},
        )


def _mock_load_connection(monkeypatch):
    conn = MagicMock()
    cur = conn.cursor.return_value.__enter__.return_value
    cur.fetchone.return_value = (0,)  # no uncertified earlier intervals
    hook = MagicMock()
    hook.get_conn.return_value = conn
    monkeypatch.setattr(load, "PostgresHook", lambda **kwargs: hook)
    monkeypatch.setattr(load, "_copy_raw", MagicMock())
    return conn, cur


def _load_kwargs():
    return {
        "table": "orders",
        "batch_id": "batch-1",
        "interval_end": datetime(2026, 1, 1, tzinfo=timezone.utc),
        "manifest": {"row_count": 2, "control_totals": {}},
        "columns": ["order_id"],
        "copy_path": "/tmp/unused.csv",
    }


def test_successful_load_commits_without_failure_audit(monkeypatch):
    conn, cur = _mock_load_connection(monkeypatch)
    monkeypatch.setattr(load, "validate_loaded_batch", MagicMock())
    audit_failure = MagicMock()
    monkeypatch.setattr(load, "_record_gate2_failure", audit_failure)

    load._load_transaction(**_load_kwargs())

    conn.commit.assert_called_once()
    conn.rollback.assert_not_called()
    audit_failure.assert_not_called()
    assert any("CERTIFIED" in str(c) for c in cur.execute.call_args_list)


def test_gate2_failure_rolls_back_and_then_audits(monkeypatch):
    conn, _ = _mock_load_connection(monkeypatch)
    error = Gate2Failure("Gate 2: expected 2 rows, found 1")
    monkeypatch.setattr(load, "validate_loaded_batch", MagicMock(side_effect=error))
    failure_audit = MagicMock()
    monkeypatch.setattr(load, "_record_gate2_failure", failure_audit)

    with pytest.raises(Gate2Failure, match="expected 2 rows"):
        load._load_transaction(**_load_kwargs())

    conn.rollback.assert_called_once()
    conn.commit.assert_not_called()
    failure_audit.assert_called_once()
    assert failure_audit.call_args.args[-1] is error


def test_operational_load_failure_is_not_mislabeled_gate2(monkeypatch):
    conn, _ = _mock_load_connection(monkeypatch)
    monkeypatch.setattr(
        load, "validate_loaded_batch", MagicMock(side_effect=RuntimeError("connection lost"))
    )
    failure_audit = MagicMock()
    monkeypatch.setattr(load, "_record_gate2_failure", failure_audit)

    with pytest.raises(RuntimeError, match="connection lost"):
        load._load_transaction(**_load_kwargs())

    conn.rollback.assert_called_once()
    conn.commit.assert_not_called()
    failure_audit.assert_not_called()
