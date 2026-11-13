"""GATE 1: validate landed Parquet/manifest before any PostgreSQL raw writes.

Each evaluated check produces an audit row. GX runs once over key/timestamp columns;
DuckDB independently checks whole-batch uniqueness. No key-range metadata.
"""

import hashlib
import tempfile
from datetime import timedelta
from decimal import Decimal
from pathlib import Path

import pyarrow.compute as pc
import pyarrow.parquet as pq

from include.common import (
    CHUNK_SIZE,
    CONTROL_COLUMNS,
    MUTABLE_KEYS,
    as_utc,
    expected_schema,
    sha256_file,
)


class Gate1Failure(ValueError):
    """Gate 1 failed one or more reported checks."""


def validate_batch_keys_duckdb(parquet_path, table):
    """GATE 1 / DUCKDB: exact uniqueness across all Parquet row groups."""
    import duckdb

    keys = MUTABLE_KEYS[table]
    selections = ", ".join('"' + key + '"' for key in keys)
    grouping = ", ".join(str(i + 1) for i in range(len(keys)))
    with tempfile.TemporaryDirectory(prefix="olist-duckdb-") as tmp:
        with duckdb.connect(database=":memory:") as db:
            db.execute("SET memory_limit='512MB'")
            db.execute("SET temp_directory = '" + tmp.replace("'", "''") + "'")
            db.execute("SET preserve_insertion_order=false")
            duplicate = db.execute(
                f"SELECT {selections}, COUNT(*) "
                "FROM read_parquet(?) "
                f"GROUP BY {grouping} HAVING COUNT(*) > 1 LIMIT 1",
                [str(parquet_path)],
            ).fetchone()
    if duplicate:
        raise ValueError(f"Duplicate business key: {duplicate[:-1]!r}")


def _gx_checks(parquet, table, manifest):
    """GATE 1 / GX: one DataFrame and one context for required keys and window."""
    import great_expectations as gx

    keys = list(MUTABLE_KEYS[table])
    frame = parquet.read(columns=keys + ["updated_at"]).to_pandas()
    context = gx.get_context(mode="ephemeral")
    source = context.data_sources.add_pandas(name="gate1_source")
    asset = source.add_dataframe_asset(name="gate1_keys_and_time")
    definition = asset.add_batch_definition_whole_dataframe("whole_file")
    batch = definition.get_batch(batch_parameters={"dataframe": frame})
    checks = {}
    for column in keys + ["updated_at"]:
        check = batch.validate(
            gx.expectations.ExpectColumnValuesToNotBeNull(column=column)
        )
        checks[f"gx_not_null_{column}"] = (
            bool(check.success),
            int(check.result.get("unexpected_count") or 0),
        )
    lower = as_utc(manifest["interval_start"]) - timedelta(
        minutes=int(manifest["overlap_minutes"])
    )
    upper = as_utc(manifest["interval_end"])
    # strictly greater than lower: GX lower bound is exclusive.
    result = batch.validate(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="updated_at",
            min_value=lower,
            max_value=upper,
            strict_min=True,
            strict_max=False,
        )
    )
    checks["gx_updated_at_window"] = (
        bool(result.success),
        int(result.result.get("unexpected_count") or 0),
    )
    return checks


def raise_error(error: Exception) -> None:
    raise error


def validate_landed_batch(
    *, manifest, parquet_path, batch_id, table, mode, interval_end, check_results=None
):
    """Return ParquetFile on success. Append one PASS/FAIL result per evaluated check."""
    checks = check_results if check_results is not None else []
    failures = []

    def run(name, fn):
        try:
            fn()
            checks.append({"check_name": name, "status": "PASS", "failed_count": 0})
            return True
        except Exception as e:
            message = str(e)[:1000]
            checks.append(
                {
                    "check_name": name,
                    "status": "FAIL",
                    "failed_count": 1,
                    "details": {"error": message},
                }
            )
            failures.append(f"{name}: {message}")
            return False

    def require(test, reason):
        if not test:
            raise ValueError(reason)

    if not run(
        "manifest_contract",
        lambda: require(
            isinstance(manifest, dict)
            and {
                "table",
                "batch_id",
                "mode",
                "interval_start",
                "interval_end",
                "overlap_minutes",
                "sha256",
                "row_count",
                "schema_fingerprint",
                "null_counts",
                "control_totals",
                "min_updated_at",
                "max_updated_at",
            }
            <= manifest.keys()
            and table in MUTABLE_KEYS
            and manifest["table"] == table
            and manifest["batch_id"] == batch_id
            and manifest["mode"] == mode == "incremental"
            and as_utc(manifest["interval_end"]) == as_utc(interval_end)
            and int(manifest["overlap_minutes"]) >= 0,
            "Invalid manifest fields, table, mode or interval",
        ),
    ):
        raise Gate1Failure("; ".join(failures))

    parquet_path = Path(parquet_path)
    run(
        "sha256",
        lambda: require(
            sha256_file(parquet_path) == manifest["sha256"], "Parquet SHA-256 mismatch"
        ),
    )
    # If the file cannot be opened, other file checks are not meaningful.
    try:
        parquet = pq.ParquetFile(parquet_path)
    except Exception as e:
        run("parquet_open", lambda err=e: raise_error(err))
        raise Gate1Failure("; ".join(failures)) from e
    run("parquet_open", lambda: None)

    # GATE 1 / SCHEMA: compare against authoritative per-table schema, not file names.
    actual_schema = parquet.schema_arrow
    run(
        "source_schema",
        lambda: require(
            actual_schema.equals(expected_schema(table), check_metadata=False),
            "Parquet columns or Arrow types differ from source contract",
        ),
    )
    run(
        "schema_fingerprint",
        lambda: require(
            hashlib.sha256(actual_schema.serialize().to_pybytes()).hexdigest()
            == manifest["schema_fingerprint"],
            "Schema fingerprint mismatch",
        ),
    )
    run(
        "parquet_metadata_rows",
        lambda: require(
            parquet.metadata.num_rows == manifest["row_count"],
            "Parquet metadata row mismatch",
        ),
    )
    if not actual_schema.equals(expected_schema(table), check_metadata=False):
        raise Gate1Failure("; ".join(failures))

    # GATE 1 / FILE STATISTICS: scan once, report each independent check.
    def scan_statistics():
        names = actual_schema.names
        nulls = dict.fromkeys(names, 0)
        totals = {key: Decimal(0) for key in CONTROL_COLUMNS.get(table, ())}
        count, low, high = 0, None, None
        for batch in parquet.iter_batches(batch_size=CHUNK_SIZE):
            count += batch.num_rows
            for name in names:
                nulls[name] += batch.column(
                    batch.schema.get_field_index(name)
                ).null_count
            for name in totals:
                val = pc.sum(batch.column(batch.schema.get_field_index(name))).as_py()
                totals[name] += val if val is not None else Decimal(0)
            times = batch.column(batch.schema.get_field_index("updated_at"))
            mn, mx = pc.min(times).as_py(), pc.max(times).as_py()
            if mn is not None:
                low = mn if low is None else min(low, mn)
                high = mx if high is None else max(high, mx)
        return count, nulls, totals, low, high

    try:
        count, nulls, totals, low, high = scan_statistics()
        checks.append({"check_name": "file_scan", "status": "PASS", "failed_count": 0})
        run(
            "scanned_row_count",
            lambda: require(
                count == manifest["row_count"], "Scanned row count mismatch"
            ),
        )
        run(
            "null_counts",
            lambda: require(nulls == manifest["null_counts"], "NULL counts mismatch"),
        )

        def compare_totals():
            require(
                set(manifest["control_totals"]) == set(totals),
                "Control-total fields mismatch",
            )
            for name, total in totals.items():
                require(
                    total == Decimal(manifest["control_totals"][name]),
                    f"Control total mismatch: {name}",
                )

        run("control_totals", compare_totals)
        run(
            "min_updated_at",
            lambda: require(
                (as_utc(low) if low is not None else None)
                == (
                    as_utc(manifest["min_updated_at"])
                    if manifest["min_updated_at"]
                    else None
                ),
                "min_updated_at mismatch",
            ),
        )
        run(
            "max_updated_at",
            lambda: require(
                (as_utc(high) if high is not None else None)
                == (
                    as_utc(manifest["max_updated_at"])
                    if manifest["max_updated_at"]
                    else None
                ),
                "max_updated_at mismatch",
            ),
        )
    except Exception as e:
        run("file_scan", lambda err=e: raise_error(err))

    run("duckdb_unique_keys", lambda: validate_batch_keys_duckdb(parquet_path, table))

    # GATE 1 / GX: one batch-scoped suite, not a new ephemeral context per chunk.
    try:
        gx_checks = _gx_checks(parquet, table, manifest)
        for name, (passed, failed_count) in gx_checks.items():
            checks.append(
                {
                    "check_name": name,
                    "status": "PASS" if passed else "FAIL",
                    "failed_count": failed_count,
                }
            )
            if not passed:
                failures.append(f"{name}: {failed_count} unexpected values")
    except Exception as e:
        run("gx_execution", lambda err=e: raise_error(err))

    if failures:
        raise Gate1Failure("; ".join(failures)[:2000])
    return parquet
