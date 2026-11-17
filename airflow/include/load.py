"""Load a Gate-1-certified file into raw; Gate 2 and certification share one TX."""

import csv
import hashlib
import json
import tempfile
from contextlib import closing
from pathlib import Path

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook

from include.common import (
    BUCKET,
    CHUNK_SIZE,
    MUTABLE_KEYS,
    S3_CONN_ID,
    canonical_json,
    identifier,
)
from include.gates.gate1 import Gate1Failure, validate_landed_batch
from include.gates.gate2 import Gate2Failure, validate_loaded_batch

LOADER_CONN_ID = "olist_loader"
QUARANTINE_BUCKET = "quarantine"


class PredecessorNotCertified(ValueError):
    """An earlier landed interval must be certified before this one."""



def _lookup_batch(batch_id):
    with closing(PostgresHook(postgres_conn_id=LOADER_CONN_ID).get_conn()) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """SELECT table_name, parquet_key, manifest_key,
                          logical_interval_end, extraction_mode
                   FROM audit.landed_batches WHERE batch_id = %s""",
                (batch_id,),
            )
            result = cursor.fetchone()
        conn.rollback()
    if result is None:
        raise ValueError(f"Unknown landed batch: {batch_id}")
    table, parquet_key, manifest_key, end, mode = result
    if table not in MUTABLE_KEYS or mode != "incremental":
        raise ValueError(f"Unsupported table or extraction mode: {table}/{mode}")
    return table, parquet_key, manifest_key, end, mode


def _set_status(cursor, batch_id, table, manifest, status, reason=None):
    """One helper for batch registry transitions. Run inside the caller TX."""
    cursor.execute(
        """INSERT INTO audit.batch_registry
           (batch_id, table_name, status, expected_rows,
            expected_control_totals, reason)
           VALUES (%s, %s, %s, %s, %s::jsonb, %s)
           ON CONFLICT (batch_id) DO UPDATE SET
               status = EXCLUDED.status, reason = EXCLUDED.reason,
               expected_rows = EXCLUDED.expected_rows,
               expected_control_totals = EXCLUDED.expected_control_totals,
               updated_at = NOW()""",
        (
            batch_id,
            table,
            status,
            int(manifest.get("row_count") or 0),
            json.dumps(manifest.get("control_totals") or {}),
            reason,
        ),
    )


def _record_gate1(batch_id, table, manifest, checks, failure=None, *, failure_status="FAILED"):
    """Persist all evaluated Gate 1 outcomes even when subsequent checks fail."""
    with closing(PostgresHook(postgres_conn_id=LOADER_CONN_ID).get_conn()) as conn:
        try:
            with conn.cursor() as cursor:
                for result in checks:
                    cursor.execute(
                        """
                        INSERT INTO audit.dq_results
                        (batch_id, table_name, gate, check_name, status, failed_count, details)
                        VALUES (%s, %s, 'GATE_1', %s, %s, %s, %s::jsonb)
                        ON CONFLICT (batch_id, gate, check_name)
                        DO UPDATE SET
                            status = EXCLUDED.status,
                            failed_count = EXCLUDED.failed_count,
                            details = EXCLUDED.details,
                            checked_at = CURRENT_TIMESTAMP
                        """,
                        (
                            batch_id,
                            table,
                            result["check_name"],
                            result["status"],
                            result["failed_count"],
                            json.dumps(result.get("details", {})),
                        ),
                    )
                if failure is not None:
                    _set_status(
                        cursor, batch_id, table, manifest, failure_status, str(failure)[:1000]
                    )
            conn.commit()
        except Exception:
            conn.rollback()
            raise



def _record_gate2_failure(batch_id, table, manifest, error):
    """Persist a confirmed Gate 2 contract failure after COPY TX rollback."""
    with closing(PostgresHook(postgres_conn_id=LOADER_CONN_ID).get_conn()) as conn:
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT status FROM audit.batch_registry "
                    "WHERE batch_id = %s FOR UPDATE",
                    (batch_id,),
                )
                row = cursor.fetchone()
                if row and row[0] == "CERTIFIED":
                    conn.rollback()
                    return
                _set_status(
                    cursor, batch_id, table, manifest, "FAILED", str(error)[:1000]
                )
                cursor.execute(
                    """INSERT INTO audit.dq_results
                       (batch_id, table_name, gate, check_name,
                        status, failed_count, details)
                       VALUES (%s, %s, 'GATE_2', 'load_reconciliation',
                               'FAIL', 1, %s::jsonb)
                       ON CONFLICT (batch_id, gate, check_name) DO UPDATE SET
                           status = EXCLUDED.status,
                           failed_count = EXCLUDED.failed_count,
                           details = EXCLUDED.details,
                           checked_at = CURRENT_TIMESTAMP""",
                    (batch_id, table, json.dumps({"error": str(error)[:1000]})),
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise

def _write_copy_csv(parquet, batch_id, parquet_key, copy_path):
    """Write COPY CSV with NULL distinct from empty string, one row at a time."""
    columns = parquet.schema_arrow.names
    with copy_path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.writer(stream, quoting=csv.QUOTE_NOTNULL)
        for batch in parquet.iter_batches(batch_size=CHUNK_SIZE):
            for record in batch.to_pylist():
                row_hash = hashlib.sha256(canonical_json(record)).hexdigest()
                writer.writerow(
                    [record[name] for name in columns]
                    + [batch_id, parquet_key, row_hash]
                )
    return columns


def _copy_raw(cursor, table, columns, copy_path):
    """Support whichever psycopg driver the installed Airflow provider selected."""
    column_names = columns + ["_batch_id", "_source_file", "_row_hash"]
    statement = (
        f"COPY raw.{identifier(table)} ("
        + ", ".join(identifier(col) for col in column_names)
        + ") FROM STDIN WITH (FORMAT csv, NULL '')"
    )
    with copy_path.open("rb") as stream:
        if hasattr(cursor, "copy_expert"):
            cursor.copy_expert(statement, stream)
        else:
            with cursor.copy(statement) as copy:
                for block in iter(lambda: stream.read(1024 * 1024), b""):
                    copy.write(block)


def _certify(cursor, table, batch_id, manifest, interval_end):
    """Advance certification in the exact same transaction as COPY + Gate 2."""
    cursor.execute(
        """UPDATE audit.batch_registry SET status = 'CERTIFIED',
              actual_rows = %s, actual_control_totals = %s::jsonb,
              updated_at = NOW(), certified_at = NOW(), reason = NULL
           WHERE batch_id = %s""",
        (manifest["row_count"], json.dumps(manifest["control_totals"]), batch_id),
    )
    # A recovered batch must not retain a stale Gate 2 FAIL. Keep this
    # reconciliation result atomic with raw COPY and certification.
    cursor.execute(
        """INSERT INTO audit.dq_results
           (batch_id, table_name, gate, check_name, status, failed_count, details)
           VALUES (%s, %s, 'GATE_2', 'load_reconciliation', 'PASS', 0, '{}'::jsonb)
           ON CONFLICT (batch_id, gate, check_name) DO UPDATE SET
               status = EXCLUDED.status,
               failed_count = EXCLUDED.failed_count,
               details = EXCLUDED.details,
               checked_at = CURRENT_TIMESTAMP""",
        (batch_id, table),
    )
    cursor.execute(
        """INSERT INTO audit.certified_watermarks
              (table_name, certified_high_watermark, last_certified_batch_id)
           VALUES (%s, %s, %s)
           ON CONFLICT (table_name) DO UPDATE SET
               certified_high_watermark = EXCLUDED.certified_high_watermark,
               last_certified_batch_id = EXCLUDED.last_certified_batch_id,
               updated_at = NOW()
           WHERE audit.certified_watermarks.certified_high_watermark IS NULL
              OR audit.certified_watermarks.certified_high_watermark
                 < EXCLUDED.certified_high_watermark""",
        (table, interval_end, batch_id),
    )


def _load_transaction(
    table, batch_id, interval_end, manifest, columns, copy_path
):
    try:
        with closing(PostgresHook(postgres_conn_id=LOADER_CONN_ID).get_conn()) as conn:
            try:
                with conn.cursor() as cursor:
                    # SERIALIZATION: one active load per table; gap check under same lock.
                    cursor.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (table,))
                    cursor.execute(
                        """SELECT COUNT(*) FROM audit.landed_batches AS b
                           LEFT JOIN audit.batch_registry AS r ON b.batch_id = r.batch_id
                           WHERE b.table_name = %s
                             AND b.extraction_mode = 'incremental'
                             AND b.logical_interval_end < %s
                             AND r.status IS DISTINCT FROM 'CERTIFIED'""",
                        (table, interval_end),
                    )
                    if cursor.fetchone()[0]:
                        raise PredecessorNotCertified("Earlier interval not certified; replay it first")
                    _set_status(cursor, batch_id, table, manifest, "LOADING")
                    cursor.execute(
                        f"DELETE FROM raw.{identifier(table)} WHERE _batch_id = %s",
                        (batch_id,),
                    )
                    _copy_raw(cursor, table, columns, copy_path)
                    # GATE 2: compare actual stored rows/totals to the manifest (trusted by Gate 1).
                    # Failure raises -> this transaction rolls back, including COPY and status.
                    validate_loaded_batch(
                        cursor=cursor,
                        table=table,
                        batch_id=batch_id,
                        expected_rows=int(manifest["row_count"]),
                        expected_totals=manifest["control_totals"],
                    )
                    _certify(cursor, table, batch_id, manifest, interval_end)
                conn.commit()
            except Exception:
                conn.rollback()
                raise
    except Gate2Failure as error:
        # The original connection is closed before the independent audit TX.
        _record_gate2_failure(batch_id, table, manifest, error)
        raise




def _quarantine_gate1_failure(s3, *, table, batch_id, parquet_key, manifest_key, error, checks):
    """Copy failed landing artifacts and publish reason.json LAST.

    S3 copy_object is idempotent at the key level. Do not delete immutable
    landing artifacts; publication of reason.json signals complete quarantine.
    """
    prefix = f"{table}/{batch_id}/"
    for source_key, name in ((parquet_key, "batch.parquet"), (manifest_key, "manifest.json")):
        s3.copy_object(
            Bucket=QUARANTINE_BUCKET,
            Key=prefix + name,
            CopySource={"Bucket": BUCKET, "Key": source_key},
        )
    reason = {
        "table": table, "batch_id": batch_id, "gate": "GATE_1",
        "reason": str(error)[:2000], "checks": checks,
        "landing_parquet_key": parquet_key, "landing_manifest_key": manifest_key,
    }
    s3.put_object(
        Bucket=QUARANTINE_BUCKET, Key=prefix + "reason.json",
        Body=canonical_json(reason), ContentType="application/json",
    )

def load_batch(batch_id):
    """Gate 1 -> audit -> CSV COPY -> Gate 2 -> atomic certification."""
    table, parquet_key, manifest_key, end, mode = _lookup_batch(batch_id)
    s3 = S3Hook(aws_conn_id=S3_CONN_ID).get_conn()
    response = s3.get_object(Bucket=BUCKET, Key=manifest_key)
    try:
        try:
            manifest = json.loads(response["Body"].read())
        except (json.JSONDecodeError, UnicodeDecodeError) as error:
            failure = Gate1Failure(f"manifest_json: invalid JSON: {error}")
            checks = [{
                "check_name": "manifest_json", "status": "FAIL",
                "failed_count": 1, "details": {"error": str(error)[:1000]},
            }]
            _record_gate1(batch_id, table, {}, checks)
            _quarantine_gate1_failure(
                s3, table=table, batch_id=batch_id, parquet_key=parquet_key,
                manifest_key=manifest_key, error=failure, checks=checks,
            )
            _record_gate1(
                batch_id, table, {}, checks, failure=failure,
                failure_status="QUARANTINED",
            )
            raise failure from error
    finally:
        response["Body"].close()

    with tempfile.TemporaryDirectory(prefix="olist-load-") as tmp:
        parquet_path = Path(tmp) / "batch.parquet"
        copy_path = Path(tmp) / "batch.csv"
        s3.download_file(BUCKET, parquet_key, str(parquet_path))
        checks = []
        try:
            parquet = validate_landed_batch(
                manifest=manifest,
                parquet_path=parquet_path,
                batch_id=batch_id,
                table=table,
                mode=mode,
                interval_end=end,
                check_results=checks,
            )
        except Gate1Failure as exc:
            # Preserve evaluated DQ checks even if object-store quarantine fails.
            # A failed copy/upload is operational and must remain retryable.
            _record_gate1(batch_id, table, manifest, checks)
            _quarantine_gate1_failure(
                s3, table=table, batch_id=batch_id, parquet_key=parquet_key,
                manifest_key=manifest_key, error=exc, checks=checks,
            )
            _record_gate1(
                batch_id, table, manifest, checks, failure=exc,
                failure_status="QUARANTINED",
            )
            raise
        except Exception:
            # Execution failure: keep partial results, but do not mark the
            # batch permanently FAILED while Airflow can still retry it.
            _record_gate1(batch_id, table, manifest, checks)
            raise
        _record_gate1(batch_id, table, manifest, checks)
        columns = _write_copy_csv(parquet, batch_id, parquet_key, copy_path)
        _load_transaction(
            table, batch_id, end, manifest, columns, copy_path
        )
    return {"table": table, "batch_id": batch_id, "loaded": manifest["row_count"]}
