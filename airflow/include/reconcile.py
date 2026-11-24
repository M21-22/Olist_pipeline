"""Compare current source rows against the latest certified raw state.

Continuous mode uses a safety horizon (certified watermark minus overlap).
Newer/ambiguous keys are deferred. It can flag old target-only keys as
*currently* deleted, but cannot determine when a hard delete happened, nor
recover an overwritten source version. Full mode requires a quiescent source.
Scheduled recovery uses this comparison against the same source snapshot.
"""

import hashlib
import json
from contextlib import closing, contextmanager
from datetime import timedelta

from airflow.providers.postgres.hooks.postgres import PostgresHook

from include.common import (
    CHUNK_SIZE,
    MUTABLE_KEYS,
    OVERLAP_MINUTES,
    TABLE_COLUMNS,
    as_utc,
    canonical_json,
    identifier,
)

SOURCE_CONN_ID = "olist_source"
RECONCILE_CONN_ID = "olist_reconcile"
RAW_METADATA = {"_batch_id", "_loaded_at", "_source_file", "_row_hash"}


class ReconciliationMismatch(RuntimeError):
    """Observed data drift; retrying an unchanged snapshot cannot repair it."""


def encode_key(table, row):
    values = [row[column] for column in MUTABLE_KEYS[table]]
    if len(values) == 1:
        return str(values[0])
    return json.dumps(values, separators=(", ", ": "))


def state_hash(row):
    """Hash the source row using the same canonical encoding as the loader."""
    business = {key: value for key, value in row.items() if key not in RAW_METADATA}
    return hashlib.sha256(canonical_json(business)).hexdigest()


def _hash_rows(cursor, table):
    rows_by_key = {}
    columns = [column[0] for column in cursor.description]
    while records := cursor.fetchmany(CHUNK_SIZE):
        for record in records:
            row = dict(zip(columns, record))
            key = encode_key(table, row)
            rows_by_key[key] = (state_hash(row), row["updated_at"])
    return rows_by_key


def key_sql(table, alias=None):
    columns = [identifier(key) for key in MUTABLE_KEYS[table]]
    if alias:
        columns = [f"{alias}.{column}" for column in columns]
    if len(columns) == 1:
        return f"{columns[0]}::text"
    return f"jsonb_build_array({', '.join(columns)})::text"


@contextmanager
def source_snapshot(table):
    """Keep the observed rows and any targeted re-extract in one source snapshot."""
    with closing(PostgresHook(postgres_conn_id=SOURCE_CONN_ID).get_conn()) as conn:
        try:
            with conn.cursor() as cursor:
                cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
                cursor.execute(f'SELECT MAX(updated_at), statement_timestamp() FROM shop.{identifier(table)}')
                latest, observed_at = cursor.fetchone()
            with conn.cursor(name="olist_reconcile_source") as cursor:
                cursor.execute(f'SELECT * FROM shop.{identifier(table)}')
                rows = _hash_rows(cursor, table)
            yield conn, rows, latest, observed_at
        finally:
            conn.rollback()


def _source_state(table):
    with source_snapshot(table) as (_, rows, latest, _):
        return rows, latest


def _certified_state(cursor, table):
    keys = MUTABLE_KEYS[table]
    partitions = ", ".join(f'r."{key}"' for key in keys)
    business_columns = ", ".join(identifier(column) for column, _ in TABLE_COLUMNS[table])
    with cursor.connection.cursor(name="olist_reconcile_target") as target_cursor:
        target_cursor.execute(
            f"""
            SELECT {business_columns} FROM (
                SELECT r.*, ROW_NUMBER() OVER (
                    PARTITION BY {partitions}
                    ORDER BY GREATEST(r.updated_at, b.repair_observed_at) DESC NULLS LAST,
                             b.certified_at DESC NULLS LAST,
                             r._batch_id DESC, r._row_hash DESC
                ) AS _rank
                FROM raw."{table}" AS r
                JOIN audit.batch_registry AS b ON b.batch_id = r._batch_id
                WHERE b.status = 'CERTIFIED' AND b.table_name = %s
                  AND NOT EXISTS (
                      SELECT 1 FROM audit.deleted_keys AS d
                      WHERE d.source_table = b.table_name AND d.business_key = {key_sql(table, "r")}
                  )
            ) AS ranked
            WHERE _rank = 1
            """,
            (table,),
        )
        return _hash_rows(target_cursor, table)


def compare_states(source_rows, target_rows, checkpoint, mode, *, overlap_minutes=OVERLAP_MINUTES):
    """Compare {key: (canonical_hash, updated_at)} source/target states.

    A row newer than the horizon cannot be judged yet, and a newer source
    version must not make its older target version look deleted. For old
    target-only keys the current source snapshot establishes absence, but
    *not when deletion occurred*. A source can change during separate DB reads;
    the overlap margin limits (not eliminates) that race.
    """
    if mode not in ("continuous", "full"):
        raise ValueError(f"Unknown reconciliation mode: {mode}")
    if overlap_minutes < 0:
        raise ValueError("overlap_minutes must be non-negative")
    horizon = (as_utc(checkpoint) - timedelta(minutes=overlap_minutes)) if mode == "continuous" else None
    deferred_newer = set()
    deferred_target_only = set()
    if horizon is not None:
        deferred_newer = {
            key for key, (_, updated) in source_rows.items()
            if updated is None or as_utc(updated) > horizon
        }
        deferred_target_only = {
            key for key, (_, updated) in target_rows.items()
            if updated is None or as_utc(updated) > horizon
        }

    # Defer the key on both sides if either version is newer than the horizon.
    deferred = deferred_newer | deferred_target_only
    missing = sorted(source_rows.keys() - target_rows.keys() - deferred)
    changed = sorted(
        key for key in (source_rows.keys() & target_rows.keys()) - deferred
        if source_rows[key][0] != target_rows[key][0]
    )
    deleted = sorted(target_rows.keys() - source_rows.keys() - deferred)

    # Keep historical output key for audit consumers, despite its broader
    # meaning (any target state newer than the horizon).
    return {
        "missing": missing,
        "changed": changed,
        "deleted": deleted,
        "deferred_newer": sorted(deferred_newer),
        "deferred_target_only": sorted(deferred_target_only),
        "eligible_count": len(source_rows) - len(source_rows.keys() & deferred),
        "source_count": len(source_rows),
        "target_count": len(target_rows),
        "horizon": horizon,
    }


def certified_checkpoint(cursor, table):
    cursor.execute(
        "SELECT certified_high_watermark FROM audit.certified_watermarks WHERE table_name = %s",
        (table,),
    )
    row = cursor.fetchone()
    if row is None or row[0] is None:
        raise RuntimeError(f"{table}: no certified watermark")
    return row[0]


def record_comparison(cursor, table, checkpoint, mode, result, *, remaining=None, **extra):
    """Store before/after counts; sampled keys keep audit rows bounded."""
    mismatches = sum(len(result[name]) for name in ("missing", "changed", "deleted"))
    current = result if remaining is None else remaining
    unrepaired = sum(len(current[name]) for name in ("missing", "changed", "deleted"))
    deferred = len(set(current["deferred_newer"]) | set(current["deferred_target_only"]))
    status = "MISMATCH" if unrepaired else ("DEFERRED" if deferred else "MATCH")
    details = {
        "mode": mode,
        "checkpoint": as_utc(checkpoint).isoformat(),
        "horizon": result["horizon"].isoformat() if result["horizon"] else None,
        "eligible_source_count": result["eligible_count"],
        "deferred_count": deferred,
        **extra,
    }
    for name in ("missing", "deleted", "changed", "deferred_newer", "deferred_target_only"):
        details[name] = result[name][:100]
        details[f"{name}_count"] = len(result[name])
    cursor.execute(
        """INSERT INTO audit.reconciliation
            (table_name, source_row_count, target_row_count, mismatch_count,
             unrepaired_count, status, details)
        VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb)""",
        (table, result["source_count"], result["target_count"],
         mismatches, unrepaired, status, json.dumps(details)),
    )
    return {
        "table": table, "mode": mode,
        "matched": current["eligible_count"] - len(current["missing"]) - len(current["changed"]),
        "deferred": deferred, "status": status, **extra,
    }


def compare_table(table, mode="continuous"):
    """Persist mismatch details and raise non-retryable drift signal."""
    if table not in MUTABLE_KEYS:
        raise ValueError(f"Invalid source table: {table}")
    if mode not in ("continuous", "full"):
        raise ValueError(f"Invalid reconciliation mode: {mode}")

    source_rows, latest = _source_state(table)
    with closing(PostgresHook(postgres_conn_id=RECONCILE_CONN_ID).get_conn()) as conn:
        try:
            with conn.cursor() as cursor:
                checkpoint = certified_checkpoint(cursor, table)
                if mode == "full" and latest is not None and as_utc(latest) > as_utc(checkpoint):
                    raise RuntimeError(
                        f"{table}: full reconciliation requires source writes paused "
                        "and ingestion caught up to the source snapshot"
                    )
                result = compare_states(source_rows, _certified_state(cursor, table), checkpoint, mode)
                summary = record_comparison(cursor, table, checkpoint, mode, result)
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    if summary["status"] == "MISMATCH":
        raise ReconciliationMismatch(
            f"{table}: missing={len(result['missing'])}, changed={len(result['changed'])}, "
            f"deleted={len(result['deleted'])}; deferred={summary['deferred']}"
        )
    return summary
