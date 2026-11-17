"""Compare current source rows against the latest certified raw state.

Continuous mode uses a safety horizon (certified watermark minus overlap).
Newer/ambiguous keys are deferred. It can flag old target-only keys as
*currently* deleted, but cannot determine when a hard delete happened, nor
recover an overwritten source version. Full mode requires a quiescent source.
This module records discrepancies, but never generates repair batches.
"""

import hashlib
import json
from contextlib import closing
from datetime import timedelta

from airflow.providers.postgres.hooks.postgres import PostgresHook

from include.common import (
    CHUNK_SIZE, MUTABLE_KEYS, OVERLAP_MINUTES, as_utc, canonical_json,
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


def _source_rows(cursor, table):
    rows_by_key = {}
    columns = [column[0] for column in cursor.description]
    while records := cursor.fetchmany(CHUNK_SIZE):
        for record in records:
            row = dict(zip(columns, record))
            key = encode_key(table, row)
            rows_by_key[key] = (state_hash(row), row["updated_at"])
    return rows_by_key


def _target_rows(cursor, table):
    """Use canonical hashes written at Parquet load time, not PG serialization."""
    rows_by_key = {}
    columns = [column[0] for column in cursor.description]
    while records := cursor.fetchmany(CHUNK_SIZE):
        for record in records:
            row = dict(zip(columns, record))
            key = encode_key(table, row)
            rows_by_key[key] = (row["_row_hash"], row["updated_at"])
    return rows_by_key


def _source_state(table):
    keys = MUTABLE_KEYS[table]
    order = ", ".join(f'"{key}"' for key in keys)
    with closing(PostgresHook(postgres_conn_id=SOURCE_CONN_ID).get_conn()) as conn:
        try:
            with conn.cursor() as cursor:
                cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
                cursor.execute(f'SELECT MAX(updated_at) FROM shop."{table}"')
                latest = cursor.fetchone()[0]
            with conn.cursor(name="olist_reconcile_source") as cursor:
                cursor.execute(f'SELECT * FROM shop."{table}" ORDER BY {order}')
                rows = _source_rows(cursor, table)
            conn.rollback()
            return rows, latest
        except Exception:
            conn.rollback()
            raise


def _certified_state(cursor, table):
    keys = MUTABLE_KEYS[table]
    partitions = ", ".join(f'r."{key}"' for key in keys)
    key_cols = ", ".join(f'"{key}"' for key in keys)
    cursor.execute(
        f"""
        SELECT {key_cols}, updated_at, _row_hash FROM (
            SELECT r.*, ROW_NUMBER() OVER (
                PARTITION BY {partitions}
                ORDER BY r.updated_at DESC NULLS LAST,
                         r._loaded_at DESC, r._batch_id DESC
            ) AS _rank
            FROM raw."{table}" AS r
            JOIN audit.batch_registry AS b ON b.batch_id = r._batch_id
            WHERE b.status = 'CERTIFIED' AND b.table_name = %s
        ) AS ranked
        WHERE _rank = 1
        """,
        (table,),
    )
    return _target_rows(cursor, table)


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
    eligible_source = {}
    eligible_target = {}
    deferred_newer = set()
    deferred_target_only = set()

    for key, (digest, updated) in source_rows.items():
        if mode == "continuous" and (updated is None or as_utc(updated) > horizon):
            deferred_newer.add(key)
        else:
            eligible_source[key] = digest

    for key, (digest, updated) in target_rows.items():
        if mode == "continuous" and (updated is None or as_utc(updated) > horizon):
            deferred_target_only.add(key)
        else:
            eligible_target[key] = digest

    # If either side has a newer state, do not classify the other side's old
    # version as deleted/missing/changed until both become eligible.
    deferred = deferred_newer | deferred_target_only
    compared_source = {k: v for k, v in eligible_source.items() if k not in deferred}
    compared_target = {k: v for k, v in eligible_target.items() if k not in deferred}

    missing = sorted(compared_source.keys() - compared_target.keys())
    changed = sorted(
        k for k in compared_source.keys() & compared_target.keys()
        if compared_source[k] != compared_target[k]
    )
    deleted = sorted(compared_target.keys() - compared_source.keys())

    # Keep historical output key for audit consumers, despite its broader
    # meaning (any target state newer than the horizon).
    return {
        "missing": missing,
        "changed": changed,
        "deleted": deleted,
        "deferred_newer": sorted(deferred_newer),
        "deferred_target_only": sorted(deferred_target_only),
        "eligible_count": len(compared_source),
        "source_count": len(source_rows),
        "target_count": len(target_rows),
        "horizon": horizon,
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
                cursor.execute(
                    "SELECT certified_high_watermark FROM audit.certified_watermarks WHERE table_name = %s",
                    (table,),
                )
                checkpoint_row = cursor.fetchone()
                checkpoint = checkpoint_row[0] if checkpoint_row else None
                if checkpoint is None:
                    raise RuntimeError(f"{table}: no certified watermark")
                if mode == "full" and latest is not None and as_utc(latest) > as_utc(checkpoint):
                    raise RuntimeError(
                        f"{table}: full reconciliation requires source writes paused "
                        "and ingestion caught up to the source snapshot"
                    )
                target_rows = _certified_state(cursor, table)
                result = compare_states(source_rows, target_rows, checkpoint, mode)
                mismatches = sum(len(result[k]) for k in ("missing", "changed", "deleted"))
                deferred = len(set(result["deferred_newer"]) | set(result["deferred_target_only"]))
                status = "MISMATCH" if mismatches else ("DEFERRED" if deferred else "MATCH")
                details = {
                    "mode": mode,
                    "checkpoint": as_utc(checkpoint).isoformat(),
                    "horizon": result["horizon"].isoformat() if result["horizon"] else None,
                    "eligible_source_count": result["eligible_count"],
                    "deferred_count": deferred,
                }
                for name in ("missing", "deleted", "changed", "deferred_newer", "deferred_target_only"):
                    details[name] = result[name][:100]
                    details[f"{name}_count"] = len(result[name])
                cursor.execute(
                    """INSERT INTO audit.reconciliation
                        (table_name, source_row_count, target_row_count,
                         mismatch_count, unrepaired_count, status, details)
                    VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb)""",
                    (table, result["source_count"], result["target_count"],
                     mismatches, mismatches, status, json.dumps(details)),
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    if mismatches:
        raise ReconciliationMismatch(
            f"{table}: missing={len(result['missing'])}, changed={len(result['changed'])}, "
            f"deleted={len(result['deleted'])}; deferred={deferred}; no automatic repair"
        )
    return {
        "table": table, "mode": mode,
        "matched": result["eligible_count"] - len(result["missing"]) - len(result["changed"]),
        "deferred": deferred, "status": status,
    }
