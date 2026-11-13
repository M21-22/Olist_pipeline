"""Land incremental source windows as Parquet and manifest-last S3 artifacts."""

import hashlib
import json
import tempfile
from contextlib import closing
from datetime import timedelta
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook

from include.common import (
    BUCKET,
    CHUNK_SIZE,
    CONTROL_COLUMNS,
    INTERVAL_MINUTES,
    MUTABLE_KEYS,
    OVERLAP_MINUTES,
    S3_CONN_ID,
    TABLE_COLUMNS,
    as_utc,
    canonical_json,
    expected_schema,
    identifier,
    sha256_file,
)

SOURCE_CONN_ID = "olist_source"
AUDIT_CONN_ID = "olist_audit"


def batch_id(table, end):
    """Stable identity for a table and Airflow interval end (not current watermark)."""
    if table not in MUTABLE_KEYS:
        raise ValueError(f"Unknown source table: {table}")
    return hashlib.sha256(
        canonical_json([table, as_utc(end), "incremental"])
    ).hexdigest()[:24]


def _write_parquet(table, start, end, path):
    """One REPEATABLE READ source snapshot; server-side cursor bounds memory."""
    schema = expected_schema(table)
    names = [name for name, _ in TABLE_COLUMNS[table]]
    selection = ", ".join(identifier(name) for name in names)
    key_order = ", ".join(identifier(name) for name in MUTABLE_KEYS[table])
    query = (
        f"SELECT {selection} FROM shop.{identifier(table)} "
        f"WHERE updated_at > %s AND updated_at <= %s ORDER BY {key_order}"
    )
    nulls = dict.fromkeys(names, 0)
    totals = {name: Decimal(0) for name in CONTROL_COLUMNS.get(table, ())}
    count = 0
    minimum = maximum = None

    with closing(PostgresHook(postgres_conn_id=SOURCE_CONN_ID).get_conn()) as conn:
        try:
            with conn.cursor() as control:
                control.execute(
                    "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"
                )
            # Server-side cursor fetches ordered rows without materializing the result set.
            with (
                conn.cursor(name="olist_extract") as source,
                pq.ParquetWriter(
                    str(path), schema, compression="zstd", version="2.6"
                ) as writer,
            ):
                source.itersize = CHUNK_SIZE
                source.execute(query, (start, end))
                while rows := source.fetchmany(CHUNK_SIZE):
                    arrays = [
                        pa.array(values, type=field.type)
                        for values, field in zip(zip(*rows), schema)
                    ]
                    chunk = pa.Table.from_arrays(arrays, schema=schema)
                    writer.write_table(chunk, row_group_size=CHUNK_SIZE)
                    count += chunk.num_rows
                    # Extract-time manifest statistics (not a Gate 1 validation).
                    for name in names:
                        nulls[name] += chunk.column(name).null_count
                    for name in totals:
                        value = pc.sum(chunk.column(name)).as_py()
                        totals[name] += value if value is not None else Decimal(0)
                    updated = chunk.column("updated_at")
                    low = pc.min(updated).as_py()
                    high = pc.max(updated).as_py()
                    if low is not None:
                        minimum = low if minimum is None else min(minimum, low)
                        maximum = high if maximum is None else max(maximum, high)
        finally:
            conn.rollback()
    return {
        "row_count": count,
        "null_counts": nulls,
        "control_totals": {name: str(value) for name, value in totals.items()},
        "min_updated_at": minimum,
        "max_updated_at": maximum,
        "schema_fingerprint": hashlib.sha256(
            schema.serialize().to_pybytes()
        ).hexdigest(),
    }


def _s3_hash(client, key):
    response = client.get_object(Bucket=BUCKET, Key=key)
    digest = hashlib.sha256()
    try:
        for chunk in response["Body"].iter_chunks(chunk_size=1024 * 1024):
            digest.update(chunk)
    finally:
        response["Body"].close()
    return digest.hexdigest()


def _existing_manifest(client, key):
    try:
        response = client.get_object(Bucket=BUCKET, Key=key)
    except client.exceptions.NoSuchKey:
        return None
    try:
        return response["Body"].read()
    finally:
        response["Body"].close()


def _register_batch(ident, table, end, parquet_key, manifest_key, manifest):
    """Atomically publish audit registry entry and landed extraction watermark."""
    with closing(PostgresHook(postgres_conn_id=AUDIT_CONN_ID).get_conn()) as conn:
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    """INSERT INTO audit.landed_batches
                       (batch_id, table_name, extraction_mode, logical_interval_start,
                        logical_interval_end, parquet_key, manifest_key, row_count)
                       VALUES (%s, %s, 'incremental', %s, %s, %s, %s, %s)
                       ON CONFLICT (batch_id) DO NOTHING""",
                    (
                        ident,
                        table,
                        manifest["interval_start"],
                        end,
                        parquet_key,
                        manifest_key,
                        manifest["row_count"],
                    ),
                )
                cursor.execute(
                    """INSERT INTO audit.extraction_watermarks
                       (table_name, landed_high_watermark, last_landed_batch_id)
                       VALUES (%s, %s, %s)
                       ON CONFLICT (table_name) DO UPDATE SET
                           landed_high_watermark = EXCLUDED.landed_high_watermark,
                           last_landed_batch_id = EXCLUDED.last_landed_batch_id,
                           updated_at = CURRENT_TIMESTAMP
                       WHERE audit.extraction_watermarks.landed_high_watermark IS NULL
                          OR audit.extraction_watermarks.landed_high_watermark
                             < EXCLUDED.landed_high_watermark""",
                    (table, end, ident),
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise


def land_table(
    table,
    interval_end,
    interval_minutes=INTERVAL_MINUTES,
    overlap_minutes=OVERLAP_MINUTES,
):
    """Publish Parquet, verify upload, publish manifest last, then audit/checkpoint."""
    if table not in MUTABLE_KEYS:
        raise ValueError(f"Unknown mutable table: {table}")
    if interval_minutes <= 0 or overlap_minutes < 0:
        raise ValueError("Invalid extraction interval or overlap")
    end = as_utc(interval_end)
    ident = batch_id(table, end)
    prefix = f"{table}/dt={end:%Y-%m-%d}/hour={end:%H}/{ident}"
    parquet_key, manifest_key = f"{prefix}.parquet", f"{prefix}.manifest.json"
    client = S3Hook(aws_conn_id=S3_CONN_ID).get_conn()
    existing = _existing_manifest(client, manifest_key)
    if existing is None:
        with closing(PostgresHook(postgres_conn_id=AUDIT_CONN_ID).get_conn()) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """SELECT landed_high_watermark FROM audit.extraction_watermarks
                                  WHERE table_name = %s""",
                    (table,),
                )
                prior = cursor.fetchone()
            conn.rollback()
        watermark = prior[0] if prior else None
        if watermark is not None and watermark >= end:
            raise RuntimeError(
                "Cannot create earlier interval after checkpoint advancement"
            )
        logical_start = (
            min(watermark, end - timedelta(minutes=interval_minutes))
            if watermark
            else end - timedelta(minutes=interval_minutes)
        )
        start = logical_start - timedelta(minutes=overlap_minutes)
        with tempfile.TemporaryDirectory(prefix="olist-extract-") as tmp:
            path = Path(tmp) / f"{ident}.parquet"
            stats = _write_parquet(table, start, end, path)
            manifest = {
                "table": table,
                "batch_id": ident,
                "mode": "incremental",
                "interval_start": logical_start,
                "interval_end": end,
                "overlap_minutes": overlap_minutes,
                **stats,
                "sha256": sha256_file(path),
                "extractor_version": "2",
            }
            client.upload_file(str(path), BUCKET, parquet_key)
            # PUBLICATION INTEGRITY: upload must match before manifest is published.
            if _s3_hash(client, parquet_key) != manifest["sha256"]:
                raise RuntimeError("Uploaded Parquet checksum mismatch")
            existing = canonical_json(manifest)
            client.put_object(Bucket=BUCKET, Key=manifest_key, Body=existing)
    manifest = json.loads(existing)
    if (
        manifest.get("table") != table
        or manifest.get("batch_id") != ident
        or manifest.get("mode") != "incremental"
        or as_utc(manifest["interval_end"]) != end
    ):
        raise RuntimeError("Existing manifest does not match this scheduled batch")
    if _s3_hash(client, parquet_key) != manifest["sha256"]:
        raise RuntimeError("Published Parquet checksum mismatch")
    _register_batch(ident, table, end, parquet_key, manifest_key, manifest)
    return ident
