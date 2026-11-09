"""Seed reference tables and non-lazy master data."""

from __future__ import annotations

import logging

import pandas as pd

from .apply import PRODUCT_COLS, SELLER_COLS, write_logs
from .data import FILES, Data, py, read_raw
from .db import get_state, set_state
from .defects import Defects
from .events import Plan
from .settings import DefectConfig, Settings

log = logging.getLogger("emulator.seed")

GEO_COLS = [
    "geolocation_zip_code_prefix",
    "geolocation_lat",
    "geolocation_lng",
    "geolocation_city",
    "geolocation_state",
]


def seed(
    conn,
    settings: Settings,
    cfg: DefectConfig,
    data: Data,
    plan: Plan,
    plan_id: dict,
) -> None:
    defects = Defects(cfg)

    # Prevent accidental repeated seeding.
    with conn.cursor() as cur:
        if get_state(cur, "seeded"):
            raise SystemExit("Already seeded. Run `reset` first.")

    with conn.transaction():
        with conn.cursor() as cur:
            # 1. Product category translations.
            translations = read_raw(
                settings.data_dir,
                "translation",
            )

            cur.executemany(
                "INSERT INTO shop.product_category_translation VALUES (%s, %s)",
                [
                    tuple(py(value) for value in row)
                    for row in translations.itertuples(index=False)
                ],
            )

            # 2. Geolocation data, loaded in chunks.
            path = settings.data_dir / FILES["geolocation"]

            count = 0

            copy_sql = (
                "COPY shop.geolocation "
                "(geolocation_pk, " + ", ".join(GEO_COLS) + ") FROM STDIN"
            )

            with cur.copy(copy_sql) as cp:
                for chunk in pd.read_csv(
                    path,
                    dtype=str,
                    keep_default_na=False,
                    na_values=[""],
                    encoding="utf-8-sig",
                    chunksize=100_000,
                ):
                    for row in chunk[GEO_COLS].itertuples(index=False):
                        count += 1
                        cp.write_row(
                            (
                                count,
                                *[py(value) for value in row],
                            )
                        )

            log.info("Geolocation rows loaded: %d", count)

            # 3. Non-lazy products.
            logs = []

            copy_products = (
                "COPY shop.products (" + ", ".join(PRODUCT_COLS) + ") FROM STDIN"
            )

            with cur.copy(copy_products) as cp:
                for i in range(data.products.n):
                    row = {
                        column: data.products.get(i, column) for column in PRODUCT_COLS
                    }

                    if row["product_id"] in plan.lazy_products:
                        continue

                    row, defects_found = defects.product(row)
                    logs.extend(defects_found)

                    cp.write_row([row[column] for column in PRODUCT_COLS])

            # 4. Non-lazy sellers.
            copy_sellers = (
                "COPY shop.sellers (" + ", ".join(SELLER_COLS) + ") FROM STDIN"
            )

            with cur.copy(copy_sellers) as cp:
                for i in range(data.sellers.n):
                    row = {
                        column: data.sellers.get(i, column) for column in SELLER_COLS
                    }

                    if row["seller_id"] in plan.lazy_sellers:
                        continue

                    row, defects_found = defects.seller(row)
                    logs.extend(defects_found)

                    cp.write_row([row[column] for column in SELLER_COLS])

            # 5. Persist seed-time defect ground truth.
            write_logs(cur, logs, None)

            # 6. Record initialization state.
            set_state(cur, "plan", plan_id)
            set_state(
                cur,
                "seeded",
                {
                    "geolocation_rows": count,
                    "seed_defects": len(logs),
                },
            )

    log.info(
        "Seed complete: %d seed-time defects logged",
        len(logs),
    )
