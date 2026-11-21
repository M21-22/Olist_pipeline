{{ config(
    materialized='incremental', incremental_strategy='merge',
    unique_key='item_pk', on_schema_change='fail',
    pre_hook="{{ fact_prune_invalid('stg_order_items', ['item_pk']) }}"
) }}

-- Order-item grain: item_pk is the stable operational key; order_item_id is
-- only the item's ordinal inside an order.
WITH projected AS (
    SELECT
        i.item_pk::bigint AS item_pk,
        MD5('item:' || i.item_pk::text)::text AS order_item_sk,
        i.order_id::text AS order_id,
        i.order_item_id::integer AS order_item_id,
        i.product_id::text AS product_id,
        i.seller_id::text AS seller_id,
        COALESCE(dp.product_sk, MD5('__olist_unknown_product__'))::text AS product_sk,
        COALESCE(ds.seller_sk, MD5('__olist_unknown_seller__'))::text AS seller_sk,
        COALESCE(dg.geography_sk, MD5('__olist_unknown_geography__'))::text AS seller_geography_sk,
        COALESCE(dd.date_sk, 0)::integer AS purchase_date_sk,
        COALESCE(dc.customer_sk, MD5('__olist_unknown_customer__'))::text AS customer_sk,
        i.shipping_limit_date AS shipping_limit_at,
        i.price::numeric(12, 2) AS price,
        i.freight_value::numeric(12, 2) AS freight_value,
        (i.price + i.freight_value)::numeric(14, 2) AS gross_item_amount,
        i.updated_at AS source_updated_at,
        i.source_batch_id::text AS source_batch_id
    FROM {{ ref('stg_order_items') }} AS i
    LEFT JOIN {{ ref('dim_product') }} AS dp
      ON i.product_id = dp.product_id AND dp.is_inferred IS FALSE
    LEFT JOIN {{ ref('dim_seller') }} AS ds
      ON i.seller_id = ds.seller_id AND ds.is_inferred IS FALSE
    LEFT JOIN {{ ref('dim_geography') }} AS dg
      ON ds.seller_zip_code_prefix = dg.zip_code_prefix AND dg.is_inferred IS FALSE
    LEFT JOIN {{ ref('stg_orders') }} AS o
      ON i.order_id = o.order_id AND o.is_valid IS TRUE
    LEFT JOIN {{ ref('stg_customers') }} AS c
      ON o.customer_id = c.customer_id AND c.is_valid IS TRUE
    LEFT JOIN {{ ref('dim_customer') }} AS dc
      ON c.customer_unique_id = dc.customer_unique_id
      AND dc.is_inferred IS FALSE
      AND o.order_purchase_timestamp >= dc.valid_from
      AND (o.order_purchase_timestamp < dc.valid_to OR dc.valid_to IS NULL)
    LEFT JOIN {{ ref('dim_date') }} AS dd
      ON (o.order_purchase_timestamp AT TIME ZONE 'UTC')::date = dd.calendar_date
    WHERE i.is_valid IS TRUE
),
candidate AS (
    SELECT projected.*, MD5(TO_JSONB(projected)::text)::text AS row_fingerprint
    FROM projected
)
SELECT candidate.*
FROM candidate
{{ fact_changed_rows(['item_pk']) }}
