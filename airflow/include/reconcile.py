"""Compare current source records with their latest certified raw versions."""

import hashlib
import json
from contextlib import closing

from airflow.providers.postgres.hooks.postgres import PostgresHook

from include.common import CHUNK_SIZE, MUTABLE_KEYS, canonical_json

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


def _read_rows(cursor, table):
    """Read a bounded number of rows at a time into a key-to-hash map."""
    rows_by_key = {}
    columns = [column[0] for column in cursor.description]
    while records := cursor.fetchmany(CHUNK_SIZE):
        for record in records:
            row = dict(zip(columns, record))
            row.pop("_rank", None)
            rows_by_key[encode_key(table, row)] = state_hash(row)
    return rows_by_key


def _source_state(table):
    """Obtain the current records and newest update from one source snapshot."""
    keys = MUTABLE_KEYS[table]
    key_order = ", ".join(f'"{key}"' for key in keys)
    with closing(PostgresHook(postgres_conn_id=SOURCE_CONN_ID).get_conn()) as conn:
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"
                )
                cursor.execute(f'SELECT MAX(updated_at) FROM shop."{table}"')
                latest_update = cursor.fetchone()[0]
            with conn.cursor(name="olist_reconcile_source") as cursor:
                cursor.execute(f'SELECT * FROM shop."{table}" ORDER BY {key_order}')
                rows = _read_rows(cursor, table)
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


# PERIODIC RECONCILIATION / UNIQUENESS BACKSTOP: compare source with certified raw.
# This detects missing, changed, and deleted records; it does not replace
# Gate 1 primary-key uniqueness checks or the dbt staging unique test.
def compare_table(table):
    """Report discrepancies; do not repair rows or change deletion tombstones."""
    if table not in MUTABLE_KEYS:
        raise ValueError(f"Invalid source table: {table}")

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
                if latest_update is not None and latest_update > checkpoint:
                    raise RuntimeError(
                        f"{table}: source has updates newer than certification"
                    )
                target_rows = _certified_state(cursor, table)

                source_keys = source_rows.keys()
                target_keys = target_rows.keys()
                missing = sorted(source_keys - target_keys)
                deleted = sorted(target_keys - source_keys)
                changed = sorted(
                    key
                    for key in source_keys & target_keys
                    if source_rows[key] != target_rows[key]
                )
                mismatches = len(missing) + len(deleted) + len(changed)
                cursor.execute(
                    """
                    INSERT INTO audit.reconciliation
                        (table_name, bucket_key, source_row_count, target_row_count,
                         mismatch_count, unrepaired_count, status, details)
                    VALUES (%s, 'ALL', %s, %s, %s, %s, %s, %s::jsonb)
                    """,
                    (
                        table,
                        len(source_rows),
                        len(target_rows),
                        mismatches,
                        mismatches,
                        "MISMATCH" if mismatches else "MATCH",
                        json.dumps(
                            {
                                "missing": missing[:100],
                                "deleted": deleted[:100],
                                "changed": changed[:100],
                            }
                        ),
                    ),
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    if mismatches:
        raise RuntimeError(
            f"{table}: missing={len(missing)}, changed={len(changed)}, "
            f"deleted={len(deleted)}; no automatic repair"
        )
    return {"table": table, "matched": len(source_rows)}
