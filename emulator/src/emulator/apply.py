"""Apply events and record injected defects in the same transaction."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta, timezone

from psycopg.types.json import Jsonb

from .data import Data
from .defects import (
    BAD_SCORES,
    BAD_STATUSES,
    FAR_FUTURE,
    Defects,
    choose,
    hit,
    uniform,
)
from .events import (
    A_APPROVED,
    A_DELIVERED,
    A_FINAL,
    A_SHIPPED,
    F_DELETE_REVIEW,
    F_LAST_UPDATE,
    K_MASTER,
    K_ORDER_CREATE,
    K_ORDER_DELETE,
    K_ORDER_STAGE,
    K_REVIEW_ANSWER,
    K_REVIEW_CREATE,
    M_PRODUCT,
    Plan,
)
from .settings import DefectConfig

DUP_OFFSET = 1_000_000_000

CUSTOMER_COLS = [
    "customer_id",
    "customer_unique_id",
    "customer_zip_code_prefix",
    "customer_city",
    "customer_state",
]
PRODUCT_COLS = [
    "product_id",
    "product_category_name",
    "product_name_lenght",
    "product_description_lenght",
    "product_photos_qty",
    "product_weight_g",
    "product_length_cm",
    "product_height_cm",
    "product_width_cm",
]
SELLER_COLS = [
    "seller_id",
    "seller_zip_code_prefix",
    "seller_city",
    "seller_state",
]
ITEM_COLS = [
    "item_pk",
    "order_id",
    "order_item_id",
    "product_id",
    "seller_id",
    "shipping_limit_date",
    "price",
    "freight_value",
]
PAYMENT_COLS = [
    "order_id",
    "payment_sequential",
    "payment_type",
    "payment_installments",
    "payment_value",
]
REVIEW_COLS = [
    "review_pk",
    "review_id",
    "order_id",
    "review_score",
    "review_comment_title",
    "review_comment_message",
    "review_creation_date",
]


def insert_sql(table: str, columns: list[str]) -> str:
    names = ", ".join(columns)
    placeholders = ", ".join(["%s"] * len(columns))
    return f"INSERT INTO shop.{table} ({names}) VALUES ({placeholders})"


SQL_CUSTOMER = (
    insert_sql("customers", CUSTOMER_COLS) + " ON CONFLICT (customer_id) DO NOTHING"
)
SQL_PRODUCT = insert_sql("products", PRODUCT_COLS)
SQL_SELLER = insert_sql("sellers", SELLER_COLS)
SQL_ITEM = insert_sql("order_items", ITEM_COLS)
SQL_PAYMENT = insert_sql("order_payments", PAYMENT_COLS)
SQL_REVIEW = insert_sql("order_reviews", REVIEW_COLS)
SQL_ORDER = insert_sql(
    "orders",
    [
        "order_id",
        "customer_id",
        "order_status",
        "order_purchase_timestamp",
        "order_estimated_delivery_date",
    ],
)

SQL_LOG = (
    "INSERT INTO chaos.injected_defects "
    "(defect_type, table_name, pk, sim_time, details) "
    "VALUES (%s, %s, %s, %s, %s)"
)

STATUS_BY_STEP = {
    A_APPROVED: "approved",
    A_SHIPPED: "shipped",
    A_DELIVERED: "delivered",
}
COL_BY_STEP = {
    A_APPROVED: "order_approved_at",
    A_SHIPPED: "order_delivered_carrier_date",
    A_DELIVERED: "order_delivered_customer_date",
}


def iso(value):
    return value.isoformat() if hasattr(value, "isoformat") else value


def ns_iso(ns: int) -> str:
    return datetime.fromtimestamp(ns / 1e9, tz=timezone.utc).isoformat()


def write_logs(cur, logs: list, sim_time) -> None:
    if logs:
        cur.executemany(
            SQL_LOG,
            [
                (kind, table, Jsonb(pk), sim_time, Jsonb(details))
                for kind, table, pk, details in logs
            ],
        )


class Applier:
    def __init__(self, cfg: DefectConfig, data: Data, plan: Plan):
        self.cfg = cfg
        self.d = data
        self.plan = plan
        self.defects = Defects(cfg)
        self.stats = Counter()

    def sim_time(self, i: int) -> datetime:
        return datetime.fromtimestamp(
            int(self.plan.ts[i]) / 1e9,
            tz=timezone.utc,
        )

    def flush(self, cur, logs: list, sim) -> None:
        write_logs(cur, logs, sim)
        self.stats.update(log[0] for log in logs)

    def apply(self, cur, i: int) -> None:
        kind = int(self.plan.kind[i])
        ix = int(self.plan.idx[i])
        aux = int(self.plan.aux[i])
        flag = int(self.plan.flag[i])
        sim = self.sim_time(i)

        if kind == K_MASTER:
            self._master(cur, ix, aux, sim)
        elif kind == K_ORDER_CREATE:
            self._order_create(cur, ix, sim)
        elif kind == K_ORDER_STAGE:
            self._order_stage(cur, ix, aux, flag, sim)
        elif kind == K_REVIEW_CREATE:
            self._review_create(cur, ix, sim)
        elif kind == K_REVIEW_ANSWER:
            self._review_answer(cur, ix, flag, sim)
        elif kind == K_ORDER_DELETE:
            self._order_delete(cur, ix, sim)
        else:
            raise ValueError(f"Unknown event kind: {kind}")

    def _master(self, cur, ix: int, aux: int, sim) -> None:
        if aux == M_PRODUCT:
            row = {c: self.d.products.get(ix, c) for c in PRODUCT_COLS}
            first_ns, ins_ns = self.plan.lazy_products[row["product_id"]]
            row, logs = self.defects.product(row)
            cur.execute(
                SQL_PRODUCT,
                [row[c] for c in PRODUCT_COLS],
            )
            table = "products"
            pk = {"product_id": row["product_id"]}
        else:
            row = {c: self.d.sellers.get(ix, c) for c in SELLER_COLS}
            first_ns, ins_ns = self.plan.lazy_sellers[row["seller_id"]]
            row, logs = self.defects.seller(row)
            cur.execute(
                SQL_SELLER,
                [row[c] for c in SELLER_COLS],
            )
            table = "sellers"
            pk = {"seller_id": row["seller_id"]}

        logs.append(
            (
                "lazy_master",
                table,
                pk,
                {
                    "first_referenced": ns_iso(first_ns),
                    "inserted": ns_iso(ins_ns),
                },
            )
        )
        self.flush(cur, logs, sim)

    def _order_create(self, cur, ix: int, sim) -> None:
        orders = self.d.orders
        cfg = self.cfg

        oid = orders.get(ix, "order_id")
        cid = orders.get(ix, "customer_id")
        logs = []

        customer = {
            c: self.d.customers.get(self.d.cust_pos[cid], c) for c in CUSTOMER_COLS
        }
        customer, customer_logs = self.defects.customer(customer)

        cur.execute(
            SQL_CUSTOMER,
            [customer[c] for c in CUSTOMER_COLS],
        )
        if cur.rowcount == 1:
            logs.extend(customer_logs)

        purchase = orders.get(ix, "order_purchase_timestamp")
        estimated = orders.get(ix, "order_estimated_delivery_date")
        customer_ref = cid

        if hit(cfg, "null_required_field", oid):
            customer_ref = None
            logs.append(
                (
                    "null_required_field",
                    "orders",
                    {"order_id": oid},
                    {"column": "customer_id", "original": cid},
                )
            )

        if hit(cfg, "future_timestamp", oid):
            logs.append(
                (
                    "future_timestamp",
                    "orders",
                    {"order_id": oid},
                    {
                        "column": "order_estimated_delivery_date",
                        "original": iso(estimated),
                        "value": iso(FAR_FUTURE),
                    },
                )
            )
            estimated = FAR_FUTURE

        cur.execute(
            SQL_ORDER,
            (oid, customer_ref, "created", purchase, estimated),
        )

        item_rows = []

        for j in self.d.items_by_order.get(oid, ()):
            row = {c: self.d.items.get(int(j), c) for c in ITEM_COLS}
            pk = {"item_pk": row["item_pk"]}
            key = str(row["item_pk"])

            for defect, column in (
                ("negative_price", "price"),
                ("negative_freight", "freight_value"),
            ):
                if row[column] and hit(cfg, defect, key):
                    original = row[column]
                    row[column] = "-" + original.lstrip("-")
                    logs.append(
                        (
                            defect,
                            "order_items",
                            pk,
                            {
                                "original": original,
                                "value": row[column],
                            },
                        )
                    )

            values = [row[c] for c in ITEM_COLS]
            item_rows.append(values)

            if hit(cfg, "duplicate_item", key):
                duplicate = list(values)
                duplicate[0] = DUP_OFFSET + int(row["item_pk"])
                item_rows.append(duplicate)

                logs.append(
                    (
                        "duplicate_item",
                        "order_items",
                        {"item_pk": duplicate[0]},
                        {
                            "duplicates_item_pk": row["item_pk"],
                            "order_id": oid,
                            "order_item_id": row["order_item_id"],
                        },
                    )
                )

        if item_rows:
            cur.executemany(SQL_ITEM, item_rows)

        payment_rows = [
            [self.d.payments.get(int(j), c) for c in PAYMENT_COLS]
            for j in self.d.pay_by_order.get(oid, ())
        ]
        if payment_rows:
            cur.executemany(SQL_PAYMENT, payment_rows)

        self.flush(cur, logs, sim)

    def _order_stage(
        self,
        cur,
        ix: int,
        step: int,
        flag: int,
        sim,
    ) -> None:
        orders = self.d.orders
        cfg = self.cfg
        oid = orders.get(ix, "order_id")
        last = bool(flag & F_LAST_UPDATE)
        logs = []

        if step == A_FINAL:
            status = orders.get(ix, "order_status")
            column = None
            value = None
        else:
            status = STATUS_BY_STEP[step]
            column = COL_BY_STEP[step]
            value = orders.get(ix, column)

            if step == A_DELIVERED and hit(cfg, "delivered_before_purchase", oid):
                original = value
                value = orders.get(ix, "order_purchase_timestamp") - timedelta(days=3)
                logs.append(
                    (
                        "delivered_before_purchase",
                        "orders",
                        {"order_id": oid},
                        {
                            "column": column,
                            "original": iso(original),
                            "value": iso(value),
                        },
                    )
                )

        if last and hit(cfg, "unknown_order_status", oid):
            original = status
            status = choose(cfg, "bad_status", oid, BAD_STATUSES)
            logs.append(
                (
                    "unknown_order_status",
                    "orders",
                    {"order_id": oid},
                    {"original": original, "value": status},
                )
            )

        assignments = "order_status = %s"
        params = [status]

        if column:
            assignments += f", {column} = %s"
            params.append(value)

        if last and hit(cfg, "silent_update", oid):
            cur.execute(
                "SELECT updated_at FROM shop.orders WHERE order_id = %s",
                (oid,),
            )
            previous = cur.fetchone()[0]

            cur.execute("SELECT set_config('olist.skip_touch', 'on', true)")
            try:
                cur.execute(
                    f"UPDATE shop.orders SET {assignments} WHERE order_id = %s",
                    params + [oid],
                )
            finally:
                cur.execute("SELECT set_config('olist.skip_touch', 'off', true)")

            logs.append(
                (
                    "silent_update",
                    "orders",
                    {"order_id": oid},
                    {
                        "status": status,
                        "previous_updated_at": iso(previous),
                    },
                )
            )

        elif last and hit(cfg, "backdated_update", oid):
            lo, hi = cfg.params["backdate_hours"]
            seconds = 3600 * uniform(cfg, "backdate_hours", oid, lo, hi)

            cur.execute("SELECT set_config('olist.skip_touch', 'on', true)")
            try:
                cur.execute(
                    f"UPDATE shop.orders SET {assignments}, "
                    "updated_at = now() - (%s * interval '1 second') "
                    "WHERE order_id = %s",
                    params + [seconds, oid],
                )
            finally:
                cur.execute("SELECT set_config('olist.skip_touch', 'off', true)")

            logs.append(
                (
                    "backdated_update",
                    "orders",
                    {"order_id": oid},
                    {
                        "status": status,
                        "backdated_seconds": round(seconds, 3),
                    },
                )
            )
        else:
            cur.execute(
                f"UPDATE shop.orders SET {assignments} WHERE order_id = %s",
                params + [oid],
            )

        if cur.rowcount != 1:
            raise RuntimeError(f"Order {oid} missing at stage {step}")

        self.flush(cur, logs, sim)

    def _review_create(self, cur, ix: int, sim) -> None:
        row = {c: self.d.reviews.get(ix, c) for c in REVIEW_COLS}
        logs = []

        key = str(row["review_pk"])

        if hit(self.cfg, "review_score_out_of_range", key):
            original = row["review_score"]
            row["review_score"] = choose(self.cfg, "bad_score", key, BAD_SCORES)
            logs.append(
                (
                    "review_score_out_of_range",
                    "order_reviews",
                    {"review_pk": row["review_pk"]},
                    {
                        "original": original,
                        "value": row["review_score"],
                    },
                )
            )

        cur.execute(
            SQL_REVIEW,
            [row[c] for c in REVIEW_COLS],
        )
        self.flush(cur, logs, sim)

    def _review_answer(self, cur, ix: int, flag: int, sim) -> None:
        reviews = self.d.reviews
        pk = reviews.get(ix, "review_pk")

        if flag & F_DELETE_REVIEW:
            cur.execute(
                "DELETE FROM shop.order_reviews WHERE review_pk = %s",
                (pk,),
            )

            if cur.rowcount != 1:
                raise RuntimeError(f"Review {pk} missing at delete")

            self.flush(
                cur,
                [
                    (
                        "hard_delete_review",
                        "order_reviews",
                        {"review_pk": pk},
                        {
                            "review_id": reviews.get(ix, "review_id"),
                            "order_id": reviews.get(ix, "order_id"),
                        },
                    )
                ],
                sim,
            )
        else:
            cur.execute(
                "UPDATE shop.order_reviews "
                "SET review_answer_timestamp = %s "
                "WHERE review_pk = %s",
                (
                    reviews.get(ix, "review_answer_timestamp"),
                    pk,
                ),
            )

            if cur.rowcount != 1:
                raise RuntimeError(f"Review {pk} missing at answer")

    def _order_delete(self, cur, ix: int, sim) -> None:
        oid = self.d.orders.get(ix, "order_id")

        cur.execute(
            "SELECT item_pk FROM shop.order_items WHERE order_id = %s ORDER BY item_pk",
            (oid,),
        )
        items = [row[0] for row in cur.fetchall()]

        cur.execute(
            "SELECT payment_sequential FROM shop.order_payments "
            "WHERE order_id = %s ORDER BY payment_sequential",
            (oid,),
        )
        payments = [row[0] for row in cur.fetchall()]

        cur.execute(
            "SELECT review_pk FROM shop.order_reviews "
            "WHERE order_id = %s ORDER BY review_pk",
            (oid,),
        )
        reviews = [row[0] for row in cur.fetchall()]

        cur.execute(
            "DELETE FROM shop.orders WHERE order_id = %s",
            (oid,),
        )

        if cur.rowcount != 1:
            raise RuntimeError(f"Order {oid} missing at delete")

        self.flush(
            cur,
            [
                (
                    "hard_delete_order",
                    "orders",
                    {"order_id": oid},
                    {
                        "n_items": len(items),
                        "n_payments": len(payments),
                        "n_reviews": len(reviews),
                        "item_pks": items,
                        "payment_sequentials": payments,
                        "review_pks": reviews,
                    },
                )
            ],
            sim,
        )
