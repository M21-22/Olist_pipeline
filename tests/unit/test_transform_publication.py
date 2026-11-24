from pathlib import Path
from unittest.mock import MagicMock

import pytest
from include import transform


@pytest.mark.parametrize(
    ("soda_result", "expected_status", "baseline_eligible"),
    [(False, "HEALTHY", True), (True, "DEGRADED", False)],
)
def test_publication_records_success_only_after_both_gates(
    monkeypatch, soda_result, expected_status, baseline_eligible,
):
    events = []

    def state(_conn, status, reason, **options):
        events.append((status, options))

    def dbt(_conn, _run_id, gate, directory, step, *_args):
        assert isinstance(directory, Path)
        events.append((gate, step))
        return False

    def soda(*_args):
        events.append("Soda")
        return soda_result

    monkeypatch.setattr(transform, "_set_state", state)
    monkeypatch.setattr(transform, "_dbt_step", dbt)
    monkeypatch.setattr(transform, "_soda_step", soda)

    transform.run_transform(MagicMock(), "test", publish=True)

    assert events == [
        ("BLOCKED", {}),
        ("GATE_3", "staging_build"),
        ("GATE_3", "sync_rejections"),
        ("GATE_4", "marts_build"),
        "Soda",
        (expected_status, {"successful": True, "baseline_eligible": baseline_eligible}),
    ]


def test_gate3_failure_stops_publication(monkeypatch):
    state = MagicMock()
    soda = MagicMock()
    dbt = MagicMock(side_effect=RuntimeError("dbt failure"))
    monkeypatch.setattr(transform, "_set_state", state)
    monkeypatch.setattr(transform, "_dbt_step", dbt)
    monkeypatch.setattr(transform, "_soda_step", soda)

    with pytest.raises(RuntimeError, match="dbt failure"):
        transform.run_transform(MagicMock(), "test", publish=True)

    assert dbt.call_count == 1
    soda.assert_not_called()
    assert state.call_count == 2
    assert state.call_args.args[1:] == ("BLOCKED", "Failed staging_build: test")
    assert not state.call_args.kwargs
