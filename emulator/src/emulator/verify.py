"""Verify source counts, defect rates, and deterministic fingerprints."""

from __future__ import annotations

import math

import pandas as pd

from .data import FILES, Data
from .db import get_state
from .events import (
    A_DELIVERED,
    F_LAST_UPDATE,
    K_ORDER_STAGE,
    K_REVIEW_ANSWER,
    Plan,
    lookup_for,
    selected_positions,
)
from .settings import DefectConfig, Settings


TABLE_PK = {
    "customers": "customer_id",
    "orders": "order_id",
    "order_items": "item_pk",
    "order_payments": "order_id, payment_sequential",
    "order_reviews": "review_pk",
    "products": "product_id",
    "sellers": "seller_id",
}


def _count_csv_rows(settings: Settings, name: str) -> int:
    return sum(
        len(chunk)
        for chunk in pd.read_csv(
            settings.data_dir / FILES[name],
            usecols=[0],
            dtype=str,
            encoding="utf-8-sig",
            chunksize=200_000,
        )
    )


def status(conn) -> None:
    with conn.cursor() as cur:
        print("cursor:", get_state(cur, "cursor"))
        print("plan:  ", get_state(cur, "plan"))

        tables = list(TABLE_PK) + [
            "geolocation",
            "product_category_translation",
        ]

        for table in tables:
            cur.execute(f"SELECT count(*) FROM shop.{table}")
            print(f"{table:32s}{cur.fetchone()[0]:>10d}")

        cur.execute(
            "SELECT defect_type, count(*) "
            "FROM chaos.injected_defects "
            "GROUP BY defect_type ORDER BY defect_type"
        )

        print("\ninjected defects:")
        for defect_type, count in cur.fetchall():
            print(f"  {defect_type:30s}{count:>8d}")


def fingerprint(conn) -> None:
    """Hashes excluding system-time-dependent fields."""
    with conn.cursor() as cur:
        for table, pk in TABLE_PK.items():
            cur.execute(
                "SELECT count(*), "
                "coalesce(md5(string_agg("
                "md5((to_jsonb(x) - 'updated_at')::text), "
                f"'' ORDER BY {pk})), '') "
                f"FROM shop.{table} x"
            )

            count, digest = cur.fetchone()
            print(f"shop.{table:24s}{count:>9d}  {digest}")

        cur.execute(
            "SELECT count(*), "
            "coalesce(md5(string_agg("
            "md5(concat_ws('|', "
            "defect_type, table_name, pk::text, "
            "sim_time::text, "
            "(details - 'previous_updated_at')::text"
            ")), '' "
            "ORDER BY defect_type, table_name, "
            "pk::text, sim_time)), '') "
            "FROM chaos.injected_defects "
            "WHERE defect_type <> 'late_commit'"
        )

        count, digest = cur.fetchone()
        print(f"chaos.injected_defects          {count:>9d}  {digest}")


def verify(
    conn,
    settings: Settings,
    cfg: DefectConfig,
    data: Data,
    plan: Plan,
) -> bool:
    ok = True

    with conn.cursor() as cur:
        state = get_state(cur, "cursor") or {"next_event": 0}

        if state["next_event"] < len(plan):
            print(
                "INCOMPLETE: "
                f"{state['next_event']} of {len(plan)} "
                "events applied. Counts below are not final."
            )
            ok = False

        cur.execute(
            "SELECT defect_type, count(*) "
            "FROM chaos.injected_defects "
            "GROUP BY defect_type"
        )
        ground_truth = dict(cur.fetchall())

        def deleted_children(key: str) -> int:
            cur.execute(
                "SELECT coalesce("
                "sum((details->>%s)::int), 0) "
                "FROM chaos.injected_defects "
                "WHERE defect_type = 'hard_delete_order'",
                (key,),
            )
            return int(cur.fetchone()[0])

        selected_lookup = lookup_for(
            plan.selected,
            data.orders.n,
        )

        def selected_count(table) -> int:
            return int(
                (
                    selected_positions(
                        table.arr("order_id"),
                        data.order_pos,
                        selected_lookup,
                    )
                    >= 0
                ).sum()
            )

        base_items = selected_count(data.items)
        base_payments = selected_count(data.payments)
        base_reviews = selected_count(data.reviews)
        selected_orders = len(plan.selected)

        customer_ids = data.orders.arr("customer_id")[plan.selected]
        unique_customers = len(set(customer_ids))

        expected = {
            "customers": unique_customers,
            "orders": (selected_orders - ground_truth.get("hard_delete_order", 0)),
            "order_items": (
                base_items
                + ground_truth.get("duplicate_item", 0)
                - deleted_children("n_items")
            ),
            "order_payments": (base_payments - deleted_children("n_payments")),
            "order_reviews": (
                base_reviews
                - ground_truth.get("hard_delete_review", 0)
                - deleted_children("n_reviews")
            ),
            "products": data.products.n,
            "sellers": data.sellers.n,
            "product_category_translation": _count_csv_rows(settings, "translation"),
            "geolocation": _count_csv_rows(settings, "geolocation"),
        }

        print("== row counts ==")

        for table, expected_count in expected.items():
            cur.execute(f"SELECT count(*) FROM shop.{table}")
            actual = cur.fetchone()[0]

            passed = actual == expected_count
            ok &= passed

            print(
                f"{'PASS' if passed else 'FAIL'}  "
                f"{table:32s} "
                f"source={actual:>9d} "
                f"expected={expected_count:>9d}"
            )

        last_events = int(
            ((plan.kind == K_ORDER_STAGE) & ((plan.flag & F_LAST_UPDATE) > 0)).sum()
        )

        delivered_events = int(
            ((plan.kind == K_ORDER_STAGE) & (plan.aux == A_DELIVERED)).sum()
        )

        answered_reviews = int((plan.kind == K_REVIEW_ANSWER).sum())

        rate = cfg.rate

        eligible = {
            "negative_price": (base_items, rate("negative_price")),
            "negative_freight": (base_items, rate("negative_freight")),
            "duplicate_item": (base_items, rate("duplicate_item")),
            "review_score_out_of_range": (
                base_reviews,
                rate("review_score_out_of_range"),
            ),
            "future_timestamp": (selected_orders, rate("future_timestamp")),
            "null_required_field": (selected_orders, rate("null_required_field")),
            "invalid_state_code": (
                unique_customers + data.sellers.n,
                rate("invalid_state_code"),
            ),
            "delivered_before_purchase": (
                delivered_events,
                rate("delivered_before_purchase"),
            ),
            "hard_delete_order": (selected_orders, rate("hard_delete_order")),
            "hard_delete_review": (answered_reviews, rate("hard_delete_review")),
            "unknown_order_status": (last_events, rate("unknown_order_status")),
            "silent_update": (last_events, rate("silent_update")),
            "backdated_update": (
                last_events * (1 - rate("silent_update")),
                rate("backdated_update"),
            ),
        }

        print("\n== injected defects vs configured rates ==")

        for name, (population, probability) in eligible.items():
            expected_count = population * probability
            actual = ground_truth.get(name, 0)

            tolerance = 4 * math.sqrt(expected_count) + 3
            passed = abs(actual - expected_count) <= tolerance
            ok &= passed

            print(
                f"{'PASS' if passed else 'FAIL'}  "
                f"{name:28s} "
                f"injected={actual:>7d} "
                f"expected={expected_count:>10.2f}"
            )

        print(
            "\nINFO "
            f"text_noise={ground_truth.get('text_noise', 0)} "
            f"lazy_master={ground_truth.get('lazy_master', 0)} "
            f"late_commit={ground_truth.get('late_commit', 0)}"
        )

    print("\nRESULT:", "OK" if ok else "PROBLEMS FOUND")
    return ok
