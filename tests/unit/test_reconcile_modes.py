"""Horizon-aware reconciliation comparisons, independent of database runtime."""
from datetime import datetime, timedelta, timezone

import pytest
from include.reconcile import compare_states

CHECKPOINT = datetime(2026, 11, 16, 12, tzinfo=timezone.utc)
HORIZON = CHECKPOINT - timedelta(minutes=10)

def source(*rows):
    return {key: (digest, updated) for key, digest, updated in rows}


def test_continuous_defers_recent_versions_and_detects_old_target_only_key():
    rows = source(("stable", "hash-a", HORIZON), ("newer", "hash-b", CHECKPOINT))
    target = source(("stable", "hash-a", HORIZON), ("newer", "hash-old", HORIZON), ("gone", "old", HORIZON))
    result = compare_states(rows, target, CHECKPOINT, "continuous")
    assert result["changed"] == []
    assert result["deleted"] == ["gone"]
    assert result["deferred_newer"] == ["newer"]
    assert result["eligible_count"] == 1
    assert result["horizon"] == HORIZON


def test_continuous_detects_eligible_missing_and_changed():
    rows = source(("missing", "m", HORIZON - timedelta(seconds=1)), ("changed", "new", HORIZON))
    result = compare_states(rows, source(("changed", "old", HORIZON)), CHECKPOINT, "continuous")
    assert result["missing"] == ["missing"]
    assert result["changed"] == ["changed"]


def test_full_detects_deleted_and_changed():
    rows = source(("changed", "new", CHECKPOINT), ("present", "p", CHECKPOINT))
    target = source(("changed", "old", CHECKPOINT), ("present", "p", CHECKPOINT), ("deleted", "x", CHECKPOINT))
    result = compare_states(rows, target, CHECKPOINT, "full")
    assert result["changed"] == ["changed"]
    assert result["deleted"] == ["deleted"]
    assert not result["deferred_newer"]


def test_horizon_boundary_and_naive_timestamps():
    result = compare_states(source(("boundary", "x", HORIZON)), {}, CHECKPOINT, "continuous")
    assert result["missing"] == ["boundary"]
    with pytest.raises(ValueError, match="timezone-aware"):
        compare_states(source(("naive", "x", datetime(2026, 11, 16, 12))), {}, CHECKPOINT, "continuous")


def test_unknown_mode_rejected():
    with pytest.raises(ValueError, match="Unknown reconciliation mode"):
        compare_states({}, {}, CHECKPOINT, "not-a-mode")


def test_target_only_newer_than_horizon_is_deferred():
    target = source(("not-yet-certified", "x", CHECKPOINT))
    result = compare_states({}, target, CHECKPOINT, "continuous")
    assert result["deleted"] == []
    assert result["deferred_target_only"] == ["not-yet-certified"]


def test_newer_target_version_does_not_generate_false_changed():
    rows = source(("key", "old", HORIZON))
    target = source(("key", "new", CHECKPOINT))
    result = compare_states(rows, target, CHECKPOINT, "continuous")
    assert result["changed"] == []
    assert result["missing"] == []
    assert result["deferred_target_only"] == ["key"]
