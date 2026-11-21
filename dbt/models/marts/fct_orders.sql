{{ config(
    materialized='incremental', incremental_strategy='merge',
    unique_key='order_id', on_schema_change='fail',
    pre_hook="{{ fact_prune_invalid('stg_orders', ['order_id']) }}"
) }}

-- Order grain. Customer SK resolves at purchase time; geography uses the order customer record.
WITH payment_totals AS (
    SELECT order_id,
           COUNT(*)::integer AS payment_count,
           SUM(payment_value)::numeric(14, 2) AS payment_total
    FROM {{ ref('stg_order_payments') }}
    WHERE is_valid IS TRUE
    GROUP BY order_id
),
item_totals AS (
    SELECT order_id,
           COUNT(*)::integer AS item_count,
           SUM(price)::numeric(14, 2) AS item_price_total,
           SUM(freight_value)::numeric(14, 2) AS freight_total
    FROM {{ ref('stg_order_items') }}
    WHERE is_valid IS TRUE
    GROUP BY order_id
),
projected AS (
    SELECT
        MD5('order:' || o.order_id)::text AS order_sk,
        o.order_id::text AS order_id,
        o.customer_id::text AS customer_id,
        c.customer_unique_id::text AS customer_unique_id,
        COALESCE(dc.customer_sk, MD5('__olist_unknown_customer__'))::text AS customer_sk,
        COALESCE(dg.geography_sk, MD5('__olist_unknown_geography__'))::text AS customer_geography_sk,
        COALESCE(dd.date_sk, 0)::integer AS purchase_date_sk,
        o.order_status::text AS order_status,
        o.order_purchase_timestamp AS purchased_at,
        o.order_approved_at AS approved_at,
        o.order_delivered_carrier_date AS delivered_to_carrier_at,
        o.order_delivered_customer_date AS delivered_to_customer_at,
        o.order_estimated_delivery_date AS estimated_delivery_at,
        CASE WHEN o.order_delivered_customer_date IS NOT NULL AND o.order_estimated_delivery_date IS NOT NULL
            THEN EXTRACT(EPOCH FROM (o.order_delivered_customer_date - o.order_estimated_delivery_date)) / 86400.0
            ELSE NULL END::numeric AS delivery_delay_days,
        COALESCE(i.item_count, 0)::integer AS item_count,
        COALESCE(i.item_price_total, 0)::numeric(14, 2) AS item_price_total,
        COALESCE(i.freight_total, 0)::numeric(14, 2) AS freight_total,
        COALESCE(p.payment_count, 0)::integer AS payment_count,
        COALESCE(p.payment_total, 0)::numeric(14, 2) AS payment_total,
        o.updated_at AS source_updated_at,
        o.source_batch_id::text AS source_batch_id
    FROM {{ ref('stg_orders') }} AS o
    LEFT JOIN {{ ref('stg_customers') }} AS c
      ON o.customer_id = c.customer_id AND c.is_valid IS TRUE
    LEFT JOIN {{ ref('dim_customer') }} AS dc
      ON c.customer_unique_id = dc.customer_unique_id
      AND dc.is_inferred IS FALSE
      AND o.order_purchase_timestamp >= dc.valid_from
      AND (o.order_purchase_timestamp < dc.valid_to OR dc.valid_to IS NULL)
    LEFT JOIN {{ ref('dim_geography') }} AS dg
      ON c.customer_zip_code_prefix = dg.zip_code_prefix AND dg.is_inferred IS FALSE
    LEFT JOIN {{ ref('dim_date') }} AS dd
      ON (o.order_purchase_timestamp AT TIME ZONE 'UTC')::date = dd.calendar_date
    LEFT JOIN payment_totals AS p ON o.order_id = p.order_id
    LEFT JOIN item_totals AS i ON o.order_id = i.order_id
    WHERE o.is_valid IS TRUE
),
candidate AS (
    SELECT projected.*, MD5(TO_JSONB(projected)::text)::text AS row_fingerprint
    FROM projected
)
SELECT candidate.*
FROM candidate
{{ fact_changed_rows(['order_id']) }}
