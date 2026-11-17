"""Certified target hashes must come from the raw _row_hash column."""
from datetime import datetime, timezone
from unittest.mock import MagicMock
from include import reconcile


def test_certified_state_uses_stored_hash_not_pg_row_rendering(monkeypatch):
    cursor = MagicMock()
    cursor.description = [("order_id",), ("updated_at",), ("_row_hash",)]
    timestamp = datetime(2026, 11, 16, tzinfo=timezone.utc)
    cursor.fetchmany.side_effect = [[("order-1", timestamp, "stored-hash")], []]
    monkeypatch.setattr(reconcile, "state_hash", lambda row: (_ for _ in ()).throw(AssertionError("unexpected recompute")))
    result = reconcile._certified_state(cursor, "orders")
    assert result == {"order-1": ("stored-hash", timestamp)}
    sql = cursor.execute.call_args.args[0]
    assert "_row_hash" in sql
