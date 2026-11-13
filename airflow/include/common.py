"""Shared, explicit source contracts and serialization for incremental ingestion."""

import hashlib
import json
from datetime import datetime, timezone
from decimal import Decimal

import pyarrow as pa

UTC = timezone.utc
BUCKET = "landing"
S3_CONN_ID = "olist_s3"
CHUNK_SIZE = 10_000
OVERLAP_MINUTES = 10
INTERVAL_MINUTES = 15

# Source DDL column order and Arrow types. Do not infer from SELECT * or the file itself.
# Static reference snapshots are deliberately out of scope here.
STRING_TYPE = pa.string()
INT32_TYPE = pa.int32()
INT64_TYPE = pa.int64()
INT16_TYPE = pa.int16()
DECIMAL_TYPE = pa.decimal128(12, 2)
TIMESTAMP_TYPE = pa.timestamp("us", tz="UTC")

TABLE_COLUMNS = {
    "customers": [
        ("customer_id", STRING_TYPE),
        ("customer_unique_id", STRING_TYPE),
        ("customer_zip_code_prefix", STRING_TYPE),
        ("customer_city", STRING_TYPE),
        ("customer_state", STRING_TYPE),
        ("updated_at", TIMESTAMP_TYPE),
    ],
    "products": [
        ("product_id", STRING_TYPE),
        ("product_category_name", STRING_TYPE),
        ("product_name_lenght", INT32_TYPE),
        ("product_description_lenght", INT32_TYPE),
        ("product_photos_qty", INT32_TYPE),
        ("product_weight_g", INT32_TYPE),
        ("product_length_cm", INT32_TYPE),
        ("product_height_cm", INT32_TYPE),
        ("product_width_cm", INT32_TYPE),
        ("updated_at", TIMESTAMP_TYPE),
    ],
    "sellers": [
        ("seller_id", STRING_TYPE),
        ("seller_zip_code_prefix", STRING_TYPE),
        ("seller_city", STRING_TYPE),
        ("seller_state", STRING_TYPE),
        ("updated_at", TIMESTAMP_TYPE),
    ],
    "orders": [
        ("order_id", STRING_TYPE),
        ("customer_id", STRING_TYPE),
        ("order_status", STRING_TYPE),
        ("order_purchase_timestamp", TIMESTAMP_TYPE),
        ("order_approved_at", TIMESTAMP_TYPE),
        ("order_delivered_carrier_date", TIMESTAMP_TYPE),
        ("order_delivered_customer_date", TIMESTAMP_TYPE),
        ("order_estimated_delivery_date", TIMESTAMP_TYPE),
        ("updated_at", TIMESTAMP_TYPE),
    ],
    "order_items": [
        ("item_pk", INT64_TYPE),
        ("order_id", STRING_TYPE),
        ("order_item_id", INT32_TYPE),
        ("product_id", STRING_TYPE),
        ("seller_id", STRING_TYPE),
        ("shipping_limit_date", TIMESTAMP_TYPE),
        ("price", DECIMAL_TYPE),
        ("freight_value", DECIMAL_TYPE),
        ("updated_at", TIMESTAMP_TYPE),
    ],
    "order_payments": [
        ("order_id", STRING_TYPE),
        ("payment_sequential", INT32_TYPE),
        ("payment_type", STRING_TYPE),
        ("payment_installments", INT32_TYPE),
        ("payment_value", DECIMAL_TYPE),
        ("updated_at", TIMESTAMP_TYPE),
    ],
    "order_reviews": [
        ("review_pk", INT64_TYPE),
        ("review_id", STRING_TYPE),
        ("order_id", STRING_TYPE),
        ("review_score", INT16_TYPE),
        ("review_comment_title", STRING_TYPE),
        ("review_comment_message", STRING_TYPE),
        ("review_creation_date", TIMESTAMP_TYPE),
        ("review_answer_timestamp", TIMESTAMP_TYPE),
        ("updated_at", TIMESTAMP_TYPE),
    ],
}
MUTABLE_KEYS = {
    "customers": ("customer_id",),
    "orders": ("order_id",),
    "order_items": ("item_pk",),
    "order_payments": ("order_id", "payment_sequential"),
    "order_reviews": ("review_pk",),
    "products": ("product_id",),
    "sellers": ("seller_id",),
}
CONTROL_COLUMNS = {
    "order_items": ("price", "freight_value"),
    "order_payments": ("payment_value",),
}


def expected_schema(table):
    return pa.schema([pa.field(name, kind) for name, kind in TABLE_COLUMNS[table]])


def as_utc(value):
    stamp = datetime.fromisoformat(value) if isinstance(value, str) else value
    if not isinstance(stamp, datetime) or stamp.tzinfo is None:
        raise ValueError("timestamp must be timezone-aware")
    return stamp.astimezone(UTC)


def canonical_json(value):
    def convert(obj):
        if isinstance(obj, datetime):
            return as_utc(obj).isoformat()
        if isinstance(obj, Decimal):
            return format(obj, "f")
        raise TypeError(type(obj).__name__)

    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), default=convert
    ).encode("utf-8")


def sha256_file(path):
    digest = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def identifier(name):
    return '"' + name.replace('"', '""') + '"'
