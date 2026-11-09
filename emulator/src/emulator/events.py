"""Build a deterministic, chronological plan of Olist events."""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

from .data import Data
from .defects import u01, uniform
from .settings import DefectConfig

# Event kinds and their tie-break priority.
(
    K_MASTER,
    K_ORDER_CREATE,
    K_ORDER_STAGE,
    K_REVIEW_CREATE,
    K_REVIEW_ANSWER,
    K_ORDER_DELETE,
) = range(6)

# Event auxiliary values.
A_APPROVED, A_SHIPPED, A_DELIVERED, A_FINAL = 1, 2, 3, 9
M_PRODUCT, M_SELLER = 0, 1

# Event flags.
F_LAST_UPDATE = 1
F_DELETE_REVIEW = 2

NS_PER_H = 3_600_000_000_000


def _ns(values: np.ndarray) -> np.ndarray:
    return values.astype("datetime64[ns]").astype("int64")


@dataclass
class Plan:
    ts: np.ndarray
    kind: np.ndarray
    idx: np.ndarray
    aux: np.ndarray
    flag: np.ndarray
    lazy_products: dict
    lazy_sellers: dict
    selected: np.ndarray

    def __len__(self) -> int:
        return len(self.ts)


def selected_positions(
    ids,
    order_pos: dict,
    sel_lookup: np.ndarray,
) -> np.ndarray:
    """Map order IDs to positions within the selected orders."""
    full = np.fromiter(
        (order_pos.get(value, -1) for value in ids),
        dtype=np.int64,
        count=len(ids),
    )
    result = np.full(len(ids), -1, dtype=np.int64)
    valid = full >= 0
    result[valid] = sel_lookup[full[valid]]
    return result


def lookup_for(selected: np.ndarray, n_orders: int) -> np.ndarray:
    lookup = np.full(n_orders, -1, dtype=np.int64)
    lookup[selected] = np.arange(len(selected))
    return lookup


def plan_lazy(
    data: Data,
    cfg: DefectConfig,
    purchase_times: np.ndarray,
    sel_lookup: np.ndarray,
):
    """Plan delayed insertion of selected products and sellers."""
    fraction = cfg.lazy_fraction()
    if fraction <= 0:
        return {}, {}

    items = data.items
    positions = selected_positions(
        items.arr("order_id"),
        data.order_pos,
        sel_lookup,
    )
    valid = positions >= 0

    frame = pd.DataFrame(
        {
            "p": items.arr("product_id")[valid],
            "s": items.arr("seller_id")[valid],
            "t": purchase_times[positions[valid]],
        }
    )

    delay_min, delay_max = cfg.params["lazy_delay_days"]
    results = []

    for column, salt, known in (
        ("p", "lazy_product", data.prod_pos),
        ("s", "lazy_seller", data.sell_pos),
    ):
        found = {}

        for key, timestamp in frame.groupby(column)["t"].min().items():
            if key in known and u01(cfg.seed, salt, key) < fraction:
                first = int(timestamp.value)
                delay_hours = 24 * uniform(
                    cfg,
                    salt + "_delay",
                    key,
                    delay_min,
                    delay_max,
                )
                found[key] = (
                    first,
                    first + int(delay_hours * NS_PER_H),
                )

        results.append(found)

    return results[0], results[1]


def build(
    data: Data,
    cfg: DefectConfig,
    date_from=None,
    date_to=None,
) -> Plan:
    orders = data.orders
    reviews = data.reviews

    purchase_all = orders.arr("order_purchase_timestamp")
    selected_mask = np.ones(orders.n, dtype=bool)

    if date_from:
        selected_mask &= purchase_all >= np.datetime64(date_from)

    if date_to:
        selected_mask &= purchase_all < np.datetime64(date_to)

    selected = np.flatnonzero(selected_mask)
    n = len(selected)
    sel_lookup = lookup_for(selected, orders.n)

    purchase_times = purchase_all[selected]
    order_ids = orders.arr("order_id")[selected]

    parts = []

    def add(ts_ns, kind, idx, aux=0, flag=0):
        count = len(ts_ns)
        parts.append(
            (
                np.asarray(ts_ns, np.int64),
                np.full(count, kind, np.int8),
                np.asarray(idx, np.int32),
                np.broadcast_to(np.asarray(aux, np.int8), count).copy(),
                np.broadcast_to(np.asarray(flag, np.int8), count).copy(),
            )
        )

    # Order creation.
    add(_ns(purchase_times), K_ORDER_CREATE, selected)

    # Chronological order status transitions.
    previous = purchase_times.copy()
    last_step = np.zeros(n, np.int8)
    stages = {}

    for step, column in (
        (A_APPROVED, "order_approved_at"),
        (A_SHIPPED, "order_delivered_carrier_date"),
        (A_DELIVERED, "order_delivered_customer_date"),
    ):
        raw = orders.arr(column)[selected]
        present = ~np.isnat(raw)
        effective = np.where(
            present,
            np.maximum(previous, raw),
            previous,
        )
        previous = np.where(present, effective, previous)
        last_step = np.where(present, step, last_step).astype(np.int8)
        stages[step] = (present, effective)

    has_approved = stages[A_APPROVED][0]
    has_shipped = stages[A_SHIPPED][0]
    has_delivered = stages[A_DELIVERED][0]

    resulting_status = np.where(
        has_delivered,
        "delivered",
        np.where(
            has_shipped,
            "shipped",
            np.where(has_approved, "approved", "created"),
        ),
    )

    final_status = (
        pd.Series(orders.arr("order_status")[selected]).fillna("").to_numpy(dtype=str)
    )

    needs_override = resulting_status != final_status
    override_positions = np.flatnonzero(needs_override)

    offsets = np.array(
        [
            uniform(
                cfg,
                "final_offset",
                str(order_ids[position]),
                1,
                72,
            )
            for position in override_positions
        ],
        dtype=float,
    )

    final_ts = _ns(previous[override_positions]) + (offsets * NS_PER_H).astype(np.int64)

    last_ns = _ns(previous)
    last_ns[override_positions] = final_ts

    last_step = np.where(needs_override, A_FINAL, last_step).astype(np.int8)

    for step in (A_APPROVED, A_SHIPPED, A_DELIVERED):
        present, effective = stages[step]
        positions = np.flatnonzero(present)

        add(
            _ns(effective[positions]),
            K_ORDER_STAGE,
            selected[positions],
            step,
            (last_step[positions] == step).astype(np.int8) * F_LAST_UPDATE,
        )

    add(
        final_ts,
        K_ORDER_STAGE,
        selected[override_positions],
        A_FINAL,
        F_LAST_UPDATE,
    )

    # Review creation and answer events.
    review_positions = selected_positions(
        reviews.arr("order_id"),
        data.order_pos,
        sel_lookup,
    )
    review_indices = np.flatnonzero(review_positions >= 0)
    review_order_positions = review_positions[review_indices]

    base = purchase_times[review_order_positions]
    creation_raw = reviews.arr("review_creation_date")[review_indices]

    creation_effective = np.where(
        np.isnat(creation_raw),
        base,
        np.maximum(creation_raw, base),
    )

    add(
        _ns(creation_effective),
        K_REVIEW_CREATE,
        review_indices,
    )

    answer_raw = reviews.arr("review_answer_timestamp")[review_indices]
    has_answer = ~np.isnat(answer_raw)

    answer_effective = np.where(
        has_answer,
        np.maximum(creation_effective, answer_raw),
        creation_effective,
    )

    answer_indices = np.flatnonzero(has_answer)
    review_pks = reviews.arr("review_pk")[review_indices[answer_indices]]

    delete_rate = cfg.rate("hard_delete_review")
    delete_flags = np.array(
        [
            u01(cfg.seed, "hard_delete_review", str(int(pk))) < delete_rate
            for pk in review_pks
        ],
        dtype=bool,
    )

    add(
        _ns(answer_effective[answer_indices]),
        K_REVIEW_ANSWER,
        review_indices[answer_indices],
        0,
        delete_flags.astype(np.int8) * F_DELETE_REVIEW,
    )

    # Order deletion after the final associated event.
    review_latest = np.zeros(n, np.int64)

    np.maximum.at(
        review_latest,
        review_order_positions,
        _ns(creation_effective),
    )
    np.maximum.at(
        review_latest,
        review_order_positions[answer_indices],
        _ns(answer_effective[answer_indices]),
    )

    horizon = np.maximum(last_ns, review_latest)
    order_delete_rate = cfg.rate("hard_delete_order")

    delete_indices = np.flatnonzero(
        np.array(
            [
                u01(cfg.seed, "hard_delete_order", str(order_id)) < order_delete_rate
                for order_id in order_ids
            ],
            dtype=bool,
        )
    )

    add(
        horizon[delete_indices] + NS_PER_H,
        K_ORDER_DELETE,
        selected[delete_indices],
    )

    # Delayed master-data insertion.
    lazy_products, lazy_sellers = plan_lazy(data, cfg, purchase_times, sel_lookup)

    if lazy_products:
        add(
            np.array(
                [value[1] for value in lazy_products.values()],
                np.int64,
            ),
            K_MASTER,
            [data.prod_pos[key] for key in lazy_products],
            M_PRODUCT,
        )

    if lazy_sellers:
        add(
            np.array(
                [value[1] for value in lazy_sellers.values()],
                np.int64,
            ),
            K_MASTER,
            [data.sell_pos[key] for key in lazy_sellers],
            M_SELLER,
        )

    # Sort by timestamp, then kind, row index, and aux value.
    ts = np.concatenate([part[0] for part in parts])
    kind = np.concatenate([part[1] for part in parts])
    idx = np.concatenate([part[2] for part in parts])
    aux = np.concatenate([part[3] for part in parts])
    flag = np.concatenate([part[4] for part in parts])

    ordering = np.lexsort((aux, idx, kind, ts))

    return Plan(
        ts[ordering],
        kind[ordering],
        idx[ordering],
        aux[ordering],
        flag[ordering],
        lazy_products,
        lazy_sellers,
        selected,
    )
