"""Reconciliation mode behavior without any database writes."""

from datetime import datetime, timedelta, timezone

import pytest

from include.reconcile import compare_states

CHECKPOINT = datetime(2026, 11, 16, 12, tzinfo=timezone.utc)


def source(*rows):
    return {key: (digest, updated_at) for key, digest, updated_at in rows}


def test_continuous_defers_recent_source_and_target_only_keys():
    rows = source(
        ("stable", "hash-a", CHECKPOINT),
        ("newer", "hash-b", CHECKPOINT + timedelta(seconds=1)),
    )
    result = compare_states(rows, {"stable": "hash-a", "newer": "hash-old", "gone": "old"}, CHECKPOINT, "continuous")
    assert result["changed"] == []
    assert result["deleted"] == []
    assert result["deferred_newer"] == ["newer"]
    assert result["deferred_target_only"] == ["gone"]
    assert result["eligible_count"] == 1


def test_continuous_detects_eligible_missing_and_changed():
    rows = source(
        ("missing", "m", CHECKPOINT - timedelta(seconds=1)),
        ("changed", "new", CHECKPOINT),
    )
    result = compare_states(rows, {"changed": "old"}, CHECKPOINT, "continuous")
    assert result["missing"] == ["missing"]
    assert result["changed"] == ["changed"]


def test_full_detects_deleted_and_changed():
    rows = source(("changed", "new", CHECKPOINT), ("present", "p", CHECKPOINT))
    result = compare_states(rows, {"changed": "old", "present": "p", "deleted": "x"}, CHECKPOINT, "full")
    assert result["changed"] == ["changed"]
    assert result["deleted"] == ["deleted"]
    assert result["deferred_newer"] == []
    assert result["deferred_target_only"] == []


def test_exclusive_deferral_boundary_and_naive_timestamp_rejected():
    result = compare_states(source(("boundary", "x", CHECKPOINT)), {}, CHECKPOINT, "continuous")
    assert result["missing"] == ["boundary"]
    with pytest.raises(ValueError, match="timezone-aware"):
        compare_states(source(("naive", "x", datetime(2026, 11, 16, 12))), {}, CHECKPOINT, "continuous")


def test_unknown_mode_rejected():
    with pytest.raises(ValueError, match="Unknown reconciliation mode"):
        compare_states({}, {}, CHECKPOINT, "not-a-mode")
