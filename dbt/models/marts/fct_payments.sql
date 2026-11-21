{{ config(
    materialized='incremental', incremental_strategy='merge',
    unique_key=['order_id', 'payment_sequential'], on_schema_change='fail',
    pre_hook="{{ fact_prune_invalid('stg_order_payments', ['order_id', 'payment_sequential']) }}"
) }}

-- Source payment grain is (order_id, payment_sequential), not the order itself.
WITH projected AS (
    SELECT
        MD5('payment:' || p.order_id || ':' || p.payment_sequential::text)::text AS payment_sk,
        p.order_id::text AS order_id,
        p.payment_sequential::integer AS payment_sequential,
        p.payment_type::text AS payment_type,
        p.payment_installments::integer AS payment_installments,
        p.payment_value::numeric(12, 2) AS payment_value,
        COALESCE(dd.date_sk, 0)::integer AS purchase_date_sk,
        COALESCE(dc.customer_sk, MD5('__olist_unknown_customer__'))::text AS customer_sk,
        p.updated_at AS source_updated_at,
        p.source_batch_id::text AS source_batch_id
    FROM {{ ref('stg_order_payments') }} AS p
    LEFT JOIN {{ ref('stg_orders') }} AS o
      ON p.order_id = o.order_id AND o.is_valid IS TRUE
    LEFT JOIN {{ ref('stg_customers') }} AS c
      ON o.customer_id = c.customer_id AND c.is_valid IS TRUE
    LEFT JOIN {{ ref('dim_customer') }} AS dc
      ON c.customer_unique_id = dc.customer_unique_id
      AND dc.is_inferred IS FALSE
      AND o.order_purchase_timestamp >= dc.valid_from
      AND (o.order_purchase_timestamp < dc.valid_to OR dc.valid_to IS NULL)
    LEFT JOIN {{ ref('dim_date') }} AS dd
      ON (o.order_purchase_timestamp AT TIME ZONE 'UTC')::date = dd.calendar_date
    WHERE p.is_valid IS TRUE
),
candidate AS (
    SELECT projected.*, MD5(TO_JSONB(projected)::text)::text AS row_fingerprint
    FROM projected
)
SELECT candidate.*
FROM candidate
{{ fact_changed_rows(['order_id', 'payment_sequential']) }}
