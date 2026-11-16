"""Compare source records to certified raw, with continuous and full modes.

Continuous mode checks eligible current source rows, deferring newer keys and
ambiguous warehouse-only keys. Full mode requires a quiescent, caught-up source.
Neither mode repairs rows or creates deletion tombstones.
"""

import hashlib
import json
from contextlib import closing

from airflow.providers.postgres.hooks.postgres import PostgresHook

from include.common import CHUNK_SIZE, MUTABLE_KEYS, as_utc, canonical_json

SOURCE_CONN_ID = "olist_source"
RECONCILE_CONN_ID = "olist_reconcile"
RAW_METADATA = {"_batch_id", "_loaded_at", "_source_file", "_row_hash"}


def encode_key(table, row):
    """Encode business keys consistently with audit.deleted_keys."""
    values = [row[column] for column in MUTABLE_KEYS[table]]
    if len(values) == 1:
        return str(values[0])
    return json.dumps(values, separators=(", ", ": "))


def state_hash(row):
    """Hash source columns, excluding raw ingestion metadata."""
    business_row = {key: value for key, value in row.items() if key not in RAW_METADATA}
    return hashlib.sha256(canonical_json(business_row)).hexdigest()


def _read_rows(cursor, table, *, include_updated_at=False):
    """Read result rows in chunks into a key-to-hash (and timestamp) map."""
    rows_by_key = {}
    columns = [column[0] for column in cursor.description]
    while records := cursor.fetchmany(CHUNK_SIZE):
        for record in records:
            row = dict(zip(columns, record))
            row.pop("_rank", None)
            key = encode_key(table, row)
            digest = state_hash(row)
            rows_by_key[key] = (digest, row["updated_at"]) if include_updated_at else digest
    return rows_by_key


def _source_state(table):
    """Read current source state and maximum updated_at from one MVCC snapshot."""
    keys = MUTABLE_KEYS[table]
    key_order = ", ".join(f'"{key}"' for key in keys)
    with closing(PostgresHook(postgres_conn_id=SOURCE_CONN_ID).get_conn()) as conn:
        try:
            with conn.cursor() as cursor:
                cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
                cursor.execute(f'SELECT MAX(updated_at) FROM shop."{table}"')
                latest_update = cursor.fetchone()[0]
            with conn.cursor(name="olist_reconcile_source") as cursor:
                cursor.execute(f'SELECT * FROM shop."{table}" ORDER BY {key_order}')
                rows = _read_rows(cursor, table, include_updated_at=True)
            conn.rollback()
            return rows, latest_update
        except Exception:
            conn.rollback()
            raise


def _certified_state(cursor, table):
    keys = MUTABLE_KEYS[table]
    partition_columns = ", ".join(f'r."{key}"' for key in keys)
    cursor.execute(
        f"""
        SELECT * FROM (
            SELECT r.*, ROW_NUMBER() OVER (
                PARTITION BY {partition_columns}
                ORDER BY r.updated_at DESC NULLS LAST,
                         r._loaded_at DESC, r._batch_id DESC
            ) AS _rank
            FROM raw."{table}" AS r
            JOIN audit.batch_registry AS b
              ON b.batch_id = r._batch_id
            WHERE b.status = 'CERTIFIED' AND b.table_name = %s
        ) AS ranked
        WHERE _rank = 1
        """,
        (table,),
    )
    return _read_rows(cursor, table)


def compare_states(source_rows, target_rows, checkpoint, mode):
    """Pure comparison; source values are (hash, updated_at) tuples.

    Current-state source tables cannot reconstruct overwritten/deleted historical
    versions. Continuous mode therefore treats target-only keys as ambiguous.
    """
    if mode not in ("continuous", "full"):
        raise ValueError(f"Unknown reconciliation mode: {mode}")

    if mode == "full":
        eligible = {key: digest for key, (digest, _) in source_rows.items()}
        deferred_newer = []
    else:
        eligible = {}
        deferred_newer = []
        for key, (digest, updated_at) in source_rows.items():
            if updated_at is None or as_utc(updated_at) > as_utc(checkpoint):
                deferred_newer.append(key)
            else:
                eligible[key] = digest

    missing = sorted(eligible.keys() - target_rows.keys())
    changed = sorted(
        key for key in eligible.keys() & target_rows.keys()
        if eligible[key] != target_rows[key]
    )
    if mode == "full":
        deleted = sorted(target_rows.keys() - source_rows.keys())
        deferred_target_only = []
    else:
        deleted = []
        # The source snapshot cannot establish *when* these keys disappeared.
        deferred_target_only = sorted(target_rows.keys() - source_rows.keys())

    return {
        "missing": missing,
        "changed": changed,
        "deleted": deleted,
        "deferred_newer": sorted(deferred_newer),
        "deferred_target_only": deferred_target_only,
        "eligible_count": len(eligible),
        "source_count": len(source_rows),
        "target_count": len(target_rows),
    }


def compare_table(table, mode="continuous"):
    """Audit drift without repairs. Full mode requires source writes to be paused."""
    if table not in MUTABLE_KEYS:
        raise ValueError(f"Invalid source table: {table}")
    if mode not in ("continuous", "full"):
        raise ValueError(f"Invalid reconciliation mode: {mode}")

    source_rows, latest_update = _source_state(table)
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
                if mode == "full" and latest_update is not None and as_utc(latest_update) > as_utc(checkpoint):
                    raise RuntimeError(
                        f"{table}: full reconciliation requires source writes paused "
                        "and ingestion caught up to the source snapshot"
                    )
                target_rows = _certified_state(cursor, table)
                result = compare_states(source_rows, target_rows, checkpoint, mode)
                mismatches = sum(len(result[k]) for k in ("missing", "changed", "deleted"))
                deferred = len(result["deferred_newer"]) + len(result["deferred_target_only"])
                status = "MISMATCH" if mismatches else ("DEFERRED" if deferred else "MATCH")
                details = {
                    "mode": mode,
                    "checkpoint": as_utc(checkpoint).isoformat(),
                    "eligible_source_count": result["eligible_count"],
                    "deferred_count": deferred,
                }
                for key in ("missing", "deleted", "changed", "deferred_newer", "deferred_target_only"):
                    details[key] = result[key][:100]
                    details[f"{key}_count"] = len(result[key])
                cursor.execute(
                    """
                    INSERT INTO audit.reconciliation
                        (table_name, source_row_count, target_row_count,
                         mismatch_count, unrepaired_count, status, details)
                    VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb)
                    """,
                    (table, result["source_count"], result["target_count"],
                     mismatches, mismatches, status, json.dumps(details)),
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    if mismatches:
        raise RuntimeError(
            f"{table}: missing={len(result['missing'])}, changed={len(result['changed'])}, "
            f"deleted={len(result['deleted'])}; deferred={deferred}; no automatic repair"
        )
    return {
        "table": table,
        "mode": mode,
        "matched": result["eligible_count"] - len(result["missing"]) - len(result["changed"]),
        "deferred": deferred,
        "status": status,
    }
