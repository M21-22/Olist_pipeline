"""Gate 1 contract regressions with real Parquet/DuckDB, no database or S3."""
import hashlib
from datetime import datetime, timedelta, timezone

import pytest
import pyarrow as pa
import pyarrow.parquet as pq

from include.common import expected_schema, sha256_file
from include.gates import gate1

END = datetime(2026, 11, 16, 12, 15, tzinfo=timezone.utc)
START = END - timedelta(minutes=15)


@pytest.fixture
def batch_factory(tmp_path, monkeypatch):
    # GX integration is covered by the existing empty-batch integration test.
    # Isolate Gate 1 collector/Arrow/DuckDB behavior here.
    monkeypatch.setattr(gate1, '_gx_checks', lambda *args: {'gx_mock': (True, 0)})

    def build(*, keys=('order-1',), row_time=None, table_name='orders'):
        schema = expected_schema('orders')
        rows = []
        for key in keys:
            row = {name: None for name in schema.names}
            row['order_id'] = key
            row['updated_at'] = row_time or END
            rows.append(row)
        arrow = pa.Table.from_pylist(rows, schema=schema)
        path = tmp_path / 'batch.parquet'
        pq.write_table(arrow, path)
        manifest = {
            'table': table_name, 'batch_id': 'batch-1', 'mode': 'incremental',
            'interval_start': START.isoformat(), 'interval_end': END.isoformat(),
            'overlap_minutes': 10, 'sha256': sha256_file(path),
            'row_count': len(rows),
            'schema_fingerprint': hashlib.sha256(schema.serialize().to_pybytes()).hexdigest(),
            'null_counts': {name: arrow.column(name).null_count for name in schema.names},
            'control_totals': {}, 'min_updated_at': (row_time or END).isoformat() if rows else None,
            'max_updated_at': (row_time or END).isoformat() if rows else None,
        }
        return path, manifest

    return build


def check(path, manifest, results=None):
    return gate1.validate_landed_batch(
        manifest=manifest, parquet_path=path, batch_id='batch-1',
        table='orders', mode='incremental', interval_end=END,
        check_results=results,
    )


def status(results, name):
    return next(item['status'] for item in results if item['check_name'] == name)


def test_valid_batch_passes_all_checks(batch_factory):
    path, manifest = batch_factory()
    results = []
    assert check(path, manifest, results).metadata.num_rows == 1
    assert len(results) > 8
    assert all(item['status'] == 'PASS' for item in results)


def test_manifest_contract_failure_short_circuits(batch_factory):
    path, manifest = batch_factory()
    manifest['batch_id'] = 'incorrect'
    results = []
    with pytest.raises(gate1.Gate1Failure, match='manifest_contract'):
        check(path, manifest, results)
    assert [item['check_name'] for item in results] == ['manifest_contract']
    assert status(results, 'manifest_contract') == 'FAIL'


def test_sha_mismatch_is_recorded_and_collected(batch_factory):
    path, manifest = batch_factory()
    manifest['sha256'] = '0' * 64
    results = []
    with pytest.raises(gate1.Gate1Failure, match='sha256'):
        check(path, manifest, results)
    assert status(results, 'sha256') == 'FAIL'
    assert status(results, 'duckdb_unique_keys') == 'PASS'


def test_row_count_mismatch_is_a_contract_failure(batch_factory):
    path, manifest = batch_factory()
    manifest['row_count'] += 1
    results = []
    with pytest.raises(gate1.Gate1Failure, match='row'):
        check(path, manifest, results)
    assert status(results, 'parquet_metadata_rows') == 'FAIL'
    assert status(results, 'scanned_row_count') == 'FAIL'


def test_duplicate_business_key_is_rejected(batch_factory):
    path, manifest = batch_factory(keys=('duplicate', 'duplicate'))
    results = []
    with pytest.raises(gate1.Gate1Failure, match='duckdb_unique_keys'):
        check(path, manifest, results)
    assert status(results, 'duckdb_unique_keys') == 'FAIL'


def test_schema_fingerprint_mismatch_is_reported(batch_factory):
    path, manifest = batch_factory()
    manifest['schema_fingerprint'] = '0' * 64
    results = []
    with pytest.raises(gate1.Gate1Failure, match='schema_fingerprint'):
        check(path, manifest, results)
    assert status(results, 'source_schema') == 'PASS'
    assert status(results, 'schema_fingerprint') == 'FAIL'


def test_duckdb_operational_failure_is_retryable(batch_factory, monkeypatch):
    path, manifest = batch_factory()
    def unavailable(*args):
        raise OSError('duckdb unavailable')
    monkeypatch.setattr(gate1, 'validate_batch_keys_duckdb', unavailable)
    results = []
    with pytest.raises(gate1.Gate1ExecutionError, match='duckdb unavailable'):
        check(path, manifest, results)
    assert status(results, 'duckdb_unique_keys') == 'ERROR'


def test_gx_operational_failure_is_retryable(batch_factory, monkeypatch):
    path, manifest = batch_factory()
    def unavailable(*args):
        raise RuntimeError('GX engine failed')
    monkeypatch.setattr(gate1, '_gx_checks', unavailable)
    results = []
    with pytest.raises(gate1.Gate1ExecutionError, match='GX engine failed'):
        check(path, manifest, results)
    assert status(results, 'gx_execution') == 'ERROR'


def test_invalid_parquet_is_permanent_contract_failure(batch_factory):
    path, manifest = batch_factory()
    path.write_bytes(b'not parquet')
    # Checksum is changed so that parquet_open is the contract check under test.
    manifest['sha256'] = sha256_file(path)
    results = []
    with pytest.raises(gate1.Gate1Failure, match='parquet_open'):
        check(path, manifest, results)
    assert status(results, 'parquet_open') == 'FAIL'


def test_empty_batch_validates_without_special_case(batch_factory):
    path, manifest = batch_factory(keys=())
    results = []
    assert check(path, manifest, results).metadata.num_rows == 0
    assert all(item['status'] == 'PASS' for item in results)


def test_gx_failed_check_is_collected_not_execution_error(batch_factory, monkeypatch):
    path, manifest = batch_factory()
    monkeypatch.setattr(gate1, '_gx_checks', lambda *args: {'gx_updated_at_window': (False, 1)})
    results = []
    with pytest.raises(gate1.Gate1Failure, match='gx_updated_at_window'):
        check(path, manifest, results)
    assert status(results, 'gx_updated_at_window') == 'FAIL'
    assert next(item for item in results if item['check_name'] == 'gx_updated_at_window')['failed_count'] == 1


def test_source_schema_mismatch_prevents_unsafe_column_scan(batch_factory):
    path, manifest = batch_factory()
    # Missing source columns must report a contract mismatch, not fail during scan.
    pq.write_table(pa.table({'order_id': ['order-1']}), path)
    manifest['sha256'] = sha256_file(path)
    results = []
    with pytest.raises(gate1.Gate1Failure, match='source_schema'):
        check(path, manifest, results)
    assert status(results, 'source_schema') == 'FAIL'
    assert all(result['check_name'] != 'file_scan' for result in results)


def test_missing_parquet_is_operational_error(batch_factory):
    path, manifest = batch_factory()
    path.unlink()
    results = []
    with pytest.raises(gate1.Gate1ExecutionError, match='sha256'):
        check(path, manifest, results)
    assert status(results, 'sha256') == 'ERROR'
