{{ config(tags=['staging_contract']) }}
-- Multi-column payment grain; no external dbt_utils dependency.
SELECT order_id, payment_sequential
FROM {{ ref('stg_order_payments') }}
GROUP BY order_id, payment_sequential
HAVING COUNT(*) > 1
