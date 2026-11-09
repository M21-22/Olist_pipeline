"""Load the Olist CSVs into compact in-memory tables."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timezone
from pathlib import Path

import numpy as np
import pandas as pd

FILES = {
    "customers": "olist_customers_dataset.csv",
    "geolocation": "olist_geolocation_dataset.csv",
    "items": "olist_order_items_dataset.csv",
    "payments": "olist_order_payments_dataset.csv",
    "reviews": "olist_order_reviews_dataset.csv",
    "orders": "olist_orders_dataset.csv",
    "products": "olist_products_dataset.csv",
    "sellers": "olist_sellers_dataset.csv",
    "translation": "product_category_name_translation.csv",
}

TS_COLS = {
    "orders": [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ],
    "items": ["shipping_limit_date"],
    "reviews": [
        "review_creation_date",
        "review_answer_timestamp",
    ],
}

INT_LIKE = {
    "items": ["order_item_id"],
    "payments": [
        "payment_sequential",
        "payment_installments",
    ],
    "reviews": ["review_score"],
    "products": [
        "product_name_lenght",
        "product_description_lenght",
        "product_photos_qty",
        "product_weight_g",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm",
    ],
}

PK_COL = {
    "items": "item_pk",
    "reviews": "review_pk",
}


def read_raw(data_dir, name: str, **kwargs) -> pd.DataFrame:
    """Read CSV values as strings, preserving leading zeros."""
    return pd.read_csv(
        Path(data_dir) / FILES[name],
        dtype=str,
        keep_default_na=False,
        na_values=[""],
        encoding="utf-8-sig",
        **kwargs,
    )


def py(value):
    """Convert pandas/numpy scalars to Python values."""
    if value is None or value is pd.NA or value is pd.NaT:
        return None

    if isinstance(value, np.datetime64):
        if np.isnat(value):
            return None
        return pd.Timestamp(value).to_pydatetime().replace(tzinfo=timezone.utc)

    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime().replace(tzinfo=timezone.utc)

    if isinstance(value, np.integer):
        return int(value)

    if isinstance(value, float):
        return None if value != value else float(value)

    return value


class Table:
    """Column-oriented, read-only DataFrame representation."""

    def __init__(self, df: pd.DataFrame):
        self.n = len(df)
        self._c = {column: df[column].to_numpy() for column in df.columns}

    def arr(self, column: str) -> np.ndarray:
        return self._c[column]

    def get(self, index: int, column: str):
        return py(self._c[column][index])


def load_table(data_dir, name: str) -> pd.DataFrame:
    df = read_raw(data_dir, name)

    for column in INT_LIKE.get(name, []):
        df[column] = df[column].str.replace(
            r"\.0+$",
            "",
            regex=True,
        )

    for column in TS_COLS.get(name, []):
        parsed = pd.to_datetime(
            df[column],
            format="%Y-%m-%d %H:%M:%S",
            errors="coerce",
        )

        bad = parsed.isna() & df[column].notna()

        if bad.any():
            raise ValueError(
                f"{name}.{column}: {int(bad.sum())} "
                "unparseable timestamps, e.g. "
                f"{df.loc[bad, column].iloc[0]!r}"
            )

        df[column] = parsed

    if name in PK_COL:
        df.insert(
            0,
            PK_COL[name],
            range(1, len(df) + 1),
        )

    return df


@dataclass
class Data:
    customers: Table
    orders: Table
    items: Table
    payments: Table
    reviews: Table
    products: Table
    sellers: Table

    cust_pos: dict
    order_pos: dict
    prod_pos: dict
    sell_pos: dict

    items_by_order: dict
    pay_by_order: dict


def load_all(data_dir) -> Data:
    names = (
        "customers",
        "orders",
        "items",
        "payments",
        "reviews",
        "products",
        "sellers",
    )

    frames = {name: load_table(data_dir, name) for name in names}

    def positions(df, column):
        return {value: index for index, value in enumerate(df[column])}

    return Data(
        customers=Table(frames["customers"]),
        orders=Table(frames["orders"]),
        items=Table(frames["items"]),
        payments=Table(frames["payments"]),
        reviews=Table(frames["reviews"]),
        products=Table(frames["products"]),
        sellers=Table(frames["sellers"]),
        cust_pos=positions(frames["customers"], "customer_id"),
        order_pos=positions(frames["orders"], "order_id"),
        prod_pos=positions(frames["products"], "product_id"),
        sell_pos=positions(frames["sellers"], "seller_id"),
        items_by_order=dict(frames["items"].groupby("order_id").indices),
        pay_by_order=dict(frames["payments"].groupby("order_id").indices),
    )
