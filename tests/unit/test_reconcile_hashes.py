"""Reconciliation verifies current payload values with canonical hashes."""
from datetime import UTC, datetime
from unittest.mock import MagicMock

from include import reconcile


def test_certified_state_hashes_current_raw_values():
    cursor = MagicMock()
    target = cursor.connection.cursor.return_value.__enter__.return_value
    target.description = [("order_id",), ("order_status",), ("updated_at",)]
    timestamp = datetime(2026, 11, 16, tzinfo=UTC)
    target.fetchmany.side_effect = [[("order-1", "shipped", timestamp)], []]

    assert reconcile._certified_state(cursor, "orders") == {
        "order-1": (reconcile.state_hash({
            "order_id": "order-1", "order_status": "shipped", "updated_at": timestamp,
        }), timestamp)
    }
    assert "order_status" in target.execute.call_args.args[0]
