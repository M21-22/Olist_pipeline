"""S3 quarantine publishes both artifacts before reason.json; tests are mocked."""
import json
from unittest.mock import MagicMock
import pytest
from include import load


def test_gate1_quarantine_copies_landing_files_and_reason_last():
    s3 = MagicMock()
    checks = [{"check_name": "checksum", "status": "FAIL", "failed_count": 1}]
    load._quarantine_gate1_failure(
        s3, table="orders", batch_id="batch-a", parquet_key="orders/x.parquet",
        manifest_key="orders/x.manifest.json", error=ValueError("bad checksum"), checks=checks,
    )
    assert s3.copy_object.call_count == 2
    first, second = (c.kwargs for c in s3.copy_object.call_args_list)
    assert first["CopySource"]["Bucket"] == "landing"
    assert first["Bucket"] == "quarantine"
    assert second["Key"].endswith("manifest.json")
    put = s3.put_object.call_args.kwargs
    assert put["Key"] == "orders/batch-a/reason.json"
    reason = json.loads(put["Body"])
    assert reason["gate"] == "GATE_1"
    assert reason["batch_id"] == "batch-a"
    assert reason["checks"] == checks


def test_failed_quarantine_copy_propagates_operational_error():
    s3 = MagicMock()
    s3.copy_object.side_effect = OSError("storage unavailable")
    with pytest.raises(OSError, match="storage unavailable"):
        load._quarantine_gate1_failure(
            s3, table="orders", batch_id="batch-a", parquet_key="p",
            manifest_key="m", error=ValueError("invalid"), checks=[],
        )
    s3.put_object.assert_not_called()


def test_load_batch_gate1_failure_quarantines_and_marks_status(monkeypatch):
    from datetime import datetime, timezone
    monkeypatch.setattr(load, "_lookup_batch", lambda batch_id: (
        "orders", "orders/input.parquet", "orders/input.manifest.json",
        datetime(2026, 11, 16, tzinfo=timezone.utc), "incremental",
    ))
    s3 = MagicMock()
    body = MagicMock()
    body.read.return_value = b'{"row_count": 3, "control_totals": {}}'
    s3.get_object.return_value = {"Body": body}
    s3.download_file.side_effect = lambda bucket, key, path: __import__("pathlib").Path(path).write_bytes(b"invalid")
    monkeypatch.setattr(load, "S3Hook", lambda **kwargs: MagicMock(get_conn=lambda: s3))

    def reject(**kwargs):
        kwargs["check_results"].append({"check_name": "parquet_open", "status": "FAIL", "failed_count": 1})
        raise load.Gate1Failure("Parquet invalid")

    monkeypatch.setattr(load, "validate_landed_batch", reject)
    record = MagicMock()
    monkeypatch.setattr(load, "_record_gate1", record)
    with pytest.raises(load.Gate1Failure, match="Parquet invalid"):
        load.load_batch("batch-a")
    assert record.call_count == 2
    assert record.call_args.kwargs["failure_status"] == "QUARANTINED"
    assert s3.copy_object.call_count == 2
    assert s3.put_object.call_count == 1


def test_quarantine_storage_failure_does_not_claim_quarantined(monkeypatch):
    from datetime import datetime, timezone
    monkeypatch.setattr(load, "_lookup_batch", lambda batch_id: (
        "orders", "orders/input.parquet", "orders/input.manifest.json",
        datetime(2026, 11, 16, tzinfo=timezone.utc), "incremental",
    ))
    s3 = MagicMock()
    body = MagicMock()
    body.read.return_value = b'{"row_count": 0}'
    s3.get_object.return_value = {"Body": body}
    s3.copy_object.side_effect = OSError("S3 unavailable")
    monkeypatch.setattr(load, "S3Hook", lambda **kwargs: MagicMock(get_conn=lambda: s3))
    monkeypatch.setattr(load, "validate_landed_batch", MagicMock(side_effect=load.Gate1Failure("corrupt")))
    record = MagicMock()
    monkeypatch.setattr(load, "_record_gate1", record)
    with pytest.raises(OSError, match="S3 unavailable"):
        load.load_batch("batch-a")
    assert record.call_count == 1
    assert record.call_args.kwargs.get("failure") is None
