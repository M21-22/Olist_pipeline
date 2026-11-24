"""Certify targeted reconciliation batches and apply observed deletions."""

from contextlib import closing
from datetime import timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from uuid import uuid4

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook

from include.common import (
    BUCKET,
    MUTABLE_KEYS,
    S3_CONN_ID,
    TABLE_COLUMNS,
    as_utc,
    canonical_json,
    identifier,
    sha256_file,
)
from include.extract import _s3_hash, write_query_parquet
from include.gates.gate1 import validate_landed_batch
from include.gates.gate2 import validate_loaded_batch
from include.load import (
    LOADER_CONN_ID,
    _certify,
    _copy_raw,
    _record_gate1,
    _set_status,
    _write_copy_csv,
)
from include.reconcile import (
    ReconciliationMismatch,
    _certified_state,
    certified_checkpoint,
    compare_states,
    key_sql,
    record_comparison,
    source_snapshot,
)


def _repair_batch(cursor, source, table, result, observed_at, batch_id):
    keys = result["missing"] + result["changed"]
    deleted = result["deleted"]
    prefix = f"{table}/repair/{batch_id}"
    parquet_key, manifest_key = f"{prefix}.parquet", f"{prefix}.manifest.json"
    selection = ", ".join(identifier(column) for column, _ in TABLE_COLUMNS[table])
    query = f"SELECT {selection} FROM shop.{identifier(table)} WHERE {key_sql(table)} = ANY(%s)"

    with TemporaryDirectory(prefix="olist-repair-") as tmp:
        path = Path(tmp) / "repair.parquet"
        stats = write_query_parquet(source, table, query, (keys,), path)
        if stats["row_count"] != len(keys):
            raise ReconciliationMismatch(f"{table}: re-extracted repair row count changed")
        lower = (stats["min_updated_at"] or observed_at) - timedelta(microseconds=1)
        manifest = {
            "table": table, "batch_id": batch_id, "mode": "repair",
            "interval_start": lower, "interval_end": observed_at,
            "overlap_minutes": 0, "extractor_version": "2",
            "deleted_keys": deleted, "sha256": sha256_file(path), **stats,
        }
        checks = []
        try:
            parquet = validate_landed_batch(
                manifest=manifest, parquet_path=path, batch_id=batch_id, table=table,
                mode="repair", interval_end=observed_at, check_results=checks,
            )
        finally:
            _record_gate1(batch_id, table, manifest, checks)

        client = S3Hook(aws_conn_id=S3_CONN_ID).get_conn()
        client.upload_file(str(path), BUCKET, parquet_key)
        if _s3_hash(client, parquet_key) != manifest["sha256"]:
            raise RuntimeError("Uploaded repair Parquet checksum mismatch")

        cursor.execute(
            """INSERT INTO audit.landed_batches
                (batch_id, table_name, extraction_mode, logical_interval_start,
                 logical_interval_end, parquet_key, manifest_key, row_count)
               VALUES (%s, %s, 'repair', %s, %s, %s, %s, %s)""",
            (batch_id, table, lower, observed_at, parquet_key, manifest_key, len(keys)),
        )
        _set_status(cursor, batch_id, table, manifest, "LOADING")
        copy_path = Path(tmp) / "repair.csv"
        cols = _write_copy_csv(parquet, batch_id, parquet_key, copy_path)
        _copy_raw(cursor, table, cols, copy_path)
        validate_loaded_batch(
            cursor=cursor, table=table, batch_id=batch_id,
            expected_rows=len(keys), expected_totals=manifest["control_totals"],
        )
        _certify(cursor, table, batch_id, manifest, observed_at, repair_observed_at=observed_at)
        cursor.execute(
            "DELETE FROM audit.deleted_keys WHERE source_table = %s AND business_key = ANY(%s)",
            (table, keys),
        )
        cursor.executemany(
            """INSERT INTO audit.deleted_keys (source_table, business_key, deleted_at)
               VALUES (%s, %s, %s) ON CONFLICT DO NOTHING""",
            [(table, key, observed_at) for key in deleted],
        )
        client.put_object(Bucket=BUCKET, Key=manifest_key, Body=canonical_json(manifest))


def repair_table(table):
    """Repair eligible drift using one consistent source snapshot per table."""
    if table not in MUTABLE_KEYS:
        raise ValueError(f"Invalid source table: {table}")
    mode = "continuous"
    with (
        closing(PostgresHook(postgres_conn_id=LOADER_CONN_ID).get_conn()) as conn,
        conn.cursor() as cursor,
    ):
        # Share publication's lock and the loader's per-table certification lock.
        cursor.execute("SELECT pg_advisory_lock(hashtextextended('olist:transform', 0))")
        cursor.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (table,))
        checkpoint = certified_checkpoint(cursor, table)
        with source_snapshot(table) as (source, rows, _, observed_at):
            target = _certified_state(cursor, table)
            result = compare_states(rows, target, checkpoint, mode)
            if not any(result[name] for name in ("missing", "changed", "deleted")):
                summary = record_comparison(cursor, table, checkpoint, mode, result)
                conn.commit()
                return summary
            batch_id = uuid4().hex
            try:
                _repair_batch(cursor, source, table, result, as_utc(observed_at), batch_id)
                remaining = compare_states(rows, _certified_state(cursor, table), checkpoint, mode)
                if any(remaining[name] for name in ("missing", "changed", "deleted")):
                    raise ReconciliationMismatch(f"{table}: eligible mismatches remain after repair")
                summary = record_comparison(
                    cursor, table, checkpoint, mode, result, remaining=remaining,
                    repair_batch_id=batch_id,
                )
                conn.commit()
            except Exception as error:
                conn.rollback()
                record_comparison(
                    cursor, table, checkpoint, mode, result,
                    failed_repair_batch_id=batch_id, error_type=type(error).__name__,
                )
                conn.commit()
                raise
    return summary
