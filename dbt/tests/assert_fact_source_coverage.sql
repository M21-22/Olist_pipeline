-- Valid source winners and published fact keys must agree in both directions.
WITH failures AS (
    SELECT 'orders' AS model, COALESCE(src.order_id, fact.order_id) AS business_key
    FROM (SELECT order_id FROM {{ ref('stg_orders') }} WHERE is_valid IS TRUE) src
    FULL JOIN {{ ref('fct_orders') }} fact USING (order_id)
    WHERE src.order_id IS NULL OR fact.order_id IS NULL
    UNION ALL
    SELECT 'items', COALESCE(src.item_pk, fact.item_pk)::text
    FROM (SELECT item_pk FROM {{ ref('stg_order_items') }} WHERE is_valid IS TRUE) src
    FULL JOIN {{ ref('fct_order_items') }} fact USING (item_pk)
    WHERE src.item_pk IS NULL OR fact.item_pk IS NULL
    UNION ALL
    SELECT 'payments', COALESCE(src.order_id, fact.order_id) || ':' ||
           COALESCE(src.payment_sequential, fact.payment_sequential)::text
    FROM (SELECT order_id, payment_sequential FROM {{ ref('stg_order_payments') }} WHERE is_valid IS TRUE) src
    FULL JOIN {{ ref('fct_payments') }} fact USING (order_id, payment_sequential)
    WHERE src.order_id IS NULL OR fact.order_id IS NULL
    UNION ALL
    SELECT 'reviews', COALESCE(src.review_pk, fact.review_pk)::text
    FROM (SELECT review_pk FROM {{ ref('stg_order_reviews') }} WHERE is_valid IS TRUE) src
    FULL JOIN {{ ref('fct_reviews') }} fact USING (review_pk)
    WHERE src.review_pk IS NULL OR fact.review_pk IS NULL
)
SELECT * FROM failures
