-- No multiplication of amounts by many-to-one dimension joins.
WITH expected_items AS (
    SELECT order_id, COUNT(*)::integer AS item_count,
           SUM(price)::numeric(14, 2) AS price_total,
           SUM(freight_value)::numeric(14, 2) AS freight_total
    FROM {{ ref('stg_order_items') }} WHERE is_valid IS TRUE GROUP BY order_id
),
expected_payments AS (
    SELECT order_id, COUNT(*)::integer AS payment_count,
           SUM(payment_value)::numeric(14, 2) AS payment_total
    FROM {{ ref('stg_order_payments') }} WHERE is_valid IS TRUE GROUP BY order_id
),
failures AS (
    SELECT o.order_id
    FROM {{ ref('fct_orders') }} o
    LEFT JOIN expected_items i USING (order_id)
    LEFT JOIN expected_payments p USING (order_id)
    WHERE o.item_count IS DISTINCT FROM COALESCE(i.item_count, 0)
       OR o.payment_count IS DISTINCT FROM COALESCE(p.payment_count, 0)
       OR o.item_price_total IS DISTINCT FROM COALESCE(i.price_total, 0)
       OR o.freight_total IS DISTINCT FROM COALESCE(i.freight_total, 0)
       OR o.payment_total IS DISTINCT FROM COALESCE(p.payment_total, 0)
)
SELECT * FROM failures
