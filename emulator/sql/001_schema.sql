-- Automatically maintain updated_at on operational tables.
-- The emulator can bypass this trigger for controlled defect injection.
CREATE OR REPLACE FUNCTION shop.touch_updated_at()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF current_setting('olist.skip_touch', true) = 'on' THEN
        RETURN NEW;
    END IF;

    NEW.updated_at := now();
    RETURN NEW;
END;
$$;

-- Master data.

CREATE TABLE shop.customers (
    customer_id text PRIMARY KEY,
    customer_unique_id text,
    customer_zip_code_prefix text,
    customer_city text,
    customer_state text,
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE shop.products (
    product_id text PRIMARY KEY,
    product_category_name text,
    product_name_lenght integer,
    product_description_lenght integer,
    product_photos_qty integer,
    product_weight_g integer,
    product_length_cm integer,
    product_height_cm integer,
    product_width_cm integer,
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE shop.sellers (
    seller_id text PRIMARY KEY,
    seller_zip_code_prefix text,
    seller_city text,
    seller_state text,
    updated_at timestamptz NOT NULL DEFAULT now()
);

-- Orders and related transactional data.

CREATE TABLE shop.orders (
    order_id text PRIMARY KEY,
    customer_id text,
    order_status text,
    order_purchase_timestamp timestamptz,
    order_approved_at timestamptz,
    order_delivered_carrier_date timestamptz,
    order_delivered_customer_date timestamptz,
    order_estimated_delivery_date timestamptz,
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE shop.order_items (
    item_pk bigint PRIMARY KEY,
    order_id text NOT NULL REFERENCES shop.orders (order_id) ON DELETE CASCADE,
    order_item_id integer,
    product_id text,
    seller_id text,
    shipping_limit_date timestamptz,
    price numeric(12, 2),
    freight_value numeric(12, 2),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX order_items_order_idx
ON shop.order_items (order_id, order_item_id);

CREATE TABLE shop.order_payments (
    order_id text NOT NULL REFERENCES shop.orders (order_id) ON DELETE CASCADE,
    payment_sequential integer NOT NULL,
    payment_type text,
    payment_installments integer,
    payment_value numeric(12, 2),
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (order_id, payment_sequential)
);

CREATE TABLE shop.order_reviews (
    review_pk bigint PRIMARY KEY,
    review_id text,
    order_id text NOT NULL REFERENCES shop.orders (order_id) ON DELETE CASCADE,
    review_score smallint,
    review_comment_title text,
    review_comment_message text,
    review_creation_date timestamptz,
    review_answer_timestamp timestamptz,
    updated_at timestamptz NOT NULL DEFAULT now()
);

-- Static reference data, refreshed in full during Phase 3.

CREATE TABLE shop.geolocation (
    geolocation_pk bigint PRIMARY KEY,
    geolocation_zip_code_prefix text,
    geolocation_lat double precision,
    geolocation_lng double precision,
    geolocation_city text,
    geolocation_state text
);

CREATE TABLE shop.product_category_translation (
    product_category_name text PRIMARY KEY,
    product_category_name_english text
);

-- Triggers and extraction indexes for mutable tables.

DO $$
DECLARE
    table_name text;
BEGIN
    FOREACH table_name IN ARRAY ARRAY[
        'customers',
        'products',
        'sellers',
        'orders',
        'order_items',
        'order_payments',
        'order_reviews'
    ]
    LOOP
        EXECUTE format(
            'CREATE TRIGGER trg_touch
             BEFORE INSERT OR UPDATE ON shop.%I
             FOR EACH ROW EXECUTE FUNCTION shop.touch_updated_at()',
            table_name
        );

        EXECUTE format(
            'CREATE INDEX %I ON shop.%I (updated_at)',
            table_name || '_updated_at_idx',
            table_name
        );
    END LOOP;
END;
$$;

-- Emulator metadata and defect ground truth.

CREATE TABLE chaos.emulator_state (
    key text PRIMARY KEY,
    value jsonb NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE chaos.injected_defects (
    defect_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    defect_type text NOT NULL,
    table_name text NOT NULL,
    pk jsonb NOT NULL,
    sim_time timestamptz,
    injected_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    details jsonb NOT NULL DEFAULT '{}'
);

CREATE INDEX injected_defects_type_idx
ON chaos.injected_defects (defect_type);

CREATE INDEX injected_defects_table_idx
ON chaos.injected_defects (table_name);
