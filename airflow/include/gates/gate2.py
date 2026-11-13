"""GATE 2: check actual raw-table content before certification in the COPY TX."""

from decimal import Decimal

from include.common import identifier


def validate_loaded_batch(*, cursor, table, batch_id, expected_rows, expected_totals):
    # GATE 2 / ROW COUNT: database rows written by this batch, not file metadata.
    cursor.execute(
        f"SELECT COUNT(*) FROM raw.{identifier(table)} WHERE _batch_id = %s",
        (batch_id,),
    )
    actual_rows = cursor.fetchone()[0]
    if actual_rows != expected_rows:
        raise ValueError(f"Gate 2: expected {expected_rows} rows, found {actual_rows}")

    # GATE 2 / CONTROL TOTALS: sum persisted numeric values; compare to manifest.
    for name, expected in expected_totals.items():
        cursor.execute(
            f"SELECT COALESCE(SUM({identifier(name)}), 0) "
            f"FROM raw.{identifier(table)} WHERE _batch_id = %s",
            (batch_id,),
        )
        actual = cursor.fetchone()[0]
        if Decimal(actual) != Decimal(expected):
            raise ValueError(f"Gate 2: persisted control-total mismatch: {name}")
