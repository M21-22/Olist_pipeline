"""Continuous reconciliation must defer in-flight updates without hiding eligible drift."""
from datetime import datetime, timedelta, timezone

from include.reconcile import compare_states

CHECKPOINT = datetime(2026, 11, 16, 12, 0, tzinfo=timezone.utc)


def test_in_flight_updates_are_deferred_while_old_discrepancies_remain_visible():
    source = {
        "same": ("hash-a", CHECKPOINT),
        "newer": ("hash-new", CHECKPOINT + timedelta(microseconds=1)),
        "missing": ("hash-c", CHECKPOINT - timedelta(seconds=1)),
        "changed": ("hash-changed", CHECKPOINT),
    }
    warehouse = {
        "same": "hash-a",
        "newer": "hash-old",
        "changed": "hash-original",
        "warehouse_only": "hash-extra",
    }
    result = compare_states(source, warehouse, CHECKPOINT, "continuous")
    assert result["deferred_newer"] == ["newer"]
    assert result["deferred_target_only"] == ["warehouse_only"]
    assert result["missing"] == ["missing"]
    assert result["changed"] == ["changed"]
    assert result["deleted"] == []
    assert result["eligible_count"] == 3


def test_after_certification_catches_up_updated_key_becomes_eligible():
    updated_at = CHECKPOINT + timedelta(seconds=1)
    source = {"newer": ("hash-new", updated_at)}
    warehouse = {"newer": "hash-new"}
    before = compare_states(source, warehouse, CHECKPOINT, "continuous")
    after = compare_states(source, warehouse, updated_at, "continuous")
    assert before["deferred_newer"] == ["newer"]
    assert after["deferred_newer"] == []
    assert after["eligible_count"] == 1
    assert after["missing"] == []
    assert after["changed"] == []
