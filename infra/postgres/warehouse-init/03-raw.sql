-- 03-raw.sql | warehouse initialization
-- Source snapshots are append-only; batch copies may contain overlapping keys.
CREATE TABLE raw.customers (
    customer_id TEXT NOT NULL,
    customer_unique_id TEXT,
    customer_zip_code_prefix TEXT,
    customer_city TEXT,
    customer_state TEXT,
    updated_at TIMESTAMPTZ,
    _batch_id TEXT NOT NULL,
    _loaded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    _source_file TEXT NOT NULL,
    _row_hash TEXT NOT NULL
);

CREATE INDEX customers_batch_idx ON raw.customers (_batch_id);

CREATE TABLE raw.orders (
    order_id TEXT NOT NULL,
    customer_id TEXT,
    order_status TEXT,
    order_purchase_timestamp TIMESTAMPTZ,
    order_approved_at TIMESTAMPTZ,
    order_delivered_carrier_date TIMESTAMPTZ,
    order_delivered_customer_date TIMESTAMPTZ,
    order_estimated_delivery_date TIMESTAMPTZ,
    updated_at TIMESTAMPTZ,
    _batch_id TEXT NOT NULL,
    _loaded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    _source_file TEXT NOT NULL,
    _row_hash TEXT NOT NULL
);

CREATE INDEX orders_batch_idx ON raw.orders (_batch_id);

CREATE TABLE raw.order_items (
    item_pk BIGINT NOT NULL,
    order_id TEXT,
    order_item_id INTEGER,
    product_id TEXT,
    seller_id TEXT,
    shipping_limit_date TIMESTAMPTZ,
    price NUMERIC(12, 2),
    freight_value NUMERIC(12, 2),
    updated_at TIMESTAMPTZ,
    _batch_id TEXT NOT NULL,
    _loaded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    _source_file TEXT NOT NULL,
    _row_hash TEXT NOT NULL
);

CREATE INDEX order_items_batch_idx ON raw.order_items (_batch_id);

CREATE TABLE raw.order_payments (
    order_id TEXT NOT NULL,
    payment_sequential INTEGER NOT NULL,
    payment_type TEXT,
    payment_installments INTEGER,
    payment_value NUMERIC(12, 2),
    updated_at TIMESTAMPTZ,
    _batch_id TEXT NOT NULL,
    _loaded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    _source_file TEXT NOT NULL,
    _row_hash TEXT NOT NULL
);

CREATE INDEX order_payments_batch_idx ON raw.order_payments (_batch_id);

CREATE TABLE raw.order_reviews (
    review_pk BIGINT NOT NULL,
    review_id TEXT,
    order_id TEXT,
    review_score SMALLINT,
    review_comment_title TEXT,
    review_comment_message TEXT,
    review_creation_date TIMESTAMPTZ,
    review_answer_timestamp TIMESTAMPTZ,
    updated_at TIMESTAMPTZ,
    _batch_id TEXT NOT NULL,
    _loaded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    _source_file TEXT NOT NULL,
    _row_hash TEXT NOT NULL
);

CREATE INDEX order_reviews_batch_idx ON raw.order_reviews (_batch_id);

CREATE TABLE raw.products (
    product_id TEXT NOT NULL,
    product_category_name TEXT,
    product_name_lenght INTEGER,
    product_description_lenght INTEGER,
    product_photos_qty INTEGER,
    product_weight_g INTEGER,
    product_length_cm INTEGER,
    product_height_cm INTEGER,
    product_width_cm INTEGER,
    updated_at TIMESTAMPTZ,
    _batch_id TEXT NOT NULL,
    _loaded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    _source_file TEXT NOT NULL,
    _row_hash TEXT NOT NULL
);

CREATE INDEX products_batch_idx ON raw.products (_batch_id);

CREATE TABLE raw.sellers (
    seller_id TEXT NOT NULL,
    seller_zip_code_prefix TEXT,
    seller_city TEXT,
    seller_state TEXT,
    updated_at TIMESTAMPTZ,
    _batch_id TEXT NOT NULL,
    _loaded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    _source_file TEXT NOT NULL,
    _row_hash TEXT NOT NULL
);

CREATE INDEX sellers_batch_idx ON raw.sellers (_batch_id);

CREATE TABLE raw.geolocation (
    geolocation_pk BIGINT NOT NULL,
    geolocation_zip_code_prefix TEXT,
    geolocation_lat DOUBLE PRECISION,
    geolocation_lng DOUBLE PRECISION,
    geolocation_city TEXT,
    geolocation_state TEXT,
    _batch_id TEXT NOT NULL,
    _loaded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    _source_file TEXT NOT NULL,
    _row_hash TEXT NOT NULL
);

CREATE INDEX geolocation_batch_idx ON raw.geolocation (_batch_id);

CREATE TABLE raw.product_category_translation (
    product_category_name TEXT NOT NULL,
    product_category_name_english TEXT,
    _batch_id TEXT NOT NULL,
    _loaded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    _source_file TEXT NOT NULL,
    _row_hash TEXT NOT NULL
);

CREATE INDEX product_category_translation_batch_idx ON raw.product_category_translation (_batch_id);
