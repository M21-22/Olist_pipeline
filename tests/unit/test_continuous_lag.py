"""Continuous reconciliation detects eligible drift without punishing in-flight versions."""
from datetime import datetime, timedelta, timezone

from include.reconcile import compare_states

CHECKPOINT = datetime(2026, 11, 16, 12, tzinfo=timezone.utc)
HORIZON = CHECKPOINT - timedelta(minutes=10)


def test_in_flight_updates_deferred_while_eligible_drift_visible():
    source = {
        "same": ("hash-a", HORIZON),
        "newer": ("hash-new", HORIZON + timedelta(microseconds=1)),
        "missing": ("hash-c", HORIZON - timedelta(seconds=1)),
        "changed": ("hash-changed", HORIZON),
    }
    warehouse = {
        "same": ("hash-a", HORIZON),
        "newer": ("hash-old", HORIZON),
        "changed": ("hash-original", HORIZON),
        "warehouse_only": ("hash-extra", HORIZON),
    }
    result = compare_states(source, warehouse, CHECKPOINT, "continuous")
    assert result["deferred_newer"] == ["newer"]
    assert result["missing"] == ["missing"]
    assert result["changed"] == ["changed"]
    assert result["deleted"] == ["warehouse_only"]
    assert result["eligible_count"] == 3


def test_after_horizon_catches_up_updated_key_becomes_eligible():
    updated = HORIZON + timedelta(seconds=1)
    source = {"newer": ("hash-new", updated)}
    warehouse = {"newer": ("hash-new", updated)}
    before = compare_states(source, warehouse, CHECKPOINT, "continuous")
    after = compare_states(source, warehouse, updated + timedelta(minutes=10), "continuous")
    assert before["deferred_newer"] == ["newer"]
    assert after["deferred_newer"] == []
    assert after["eligible_count"] == 1
    assert after["changed"] == []
