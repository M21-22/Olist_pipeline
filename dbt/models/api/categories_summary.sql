-- Current category attribution and GMV logic match mart_daily_kpis:
-- valid item prices, excluding freight and canceled/unavailable orders.
-- Payments are intentionally not joined at item grain (fan-out risk).
SELECT
    COALESCE(NULLIF(p.product_category_name_english, ''),
             NULLIF(p.product_category_name, ''), 'Unknown')::text AS category,
    COUNT(*)::bigint AS item_count,
    COUNT(DISTINCT i.order_id)::bigint AS order_count,
    COALESCE(SUM(i.price), 0)::numeric(18, 2) AS gmv,
    COALESCE(SUM(i.freight_value), 0)::numeric(18, 2) AS freight_total
FROM {{ ref('fct_order_items') }} AS i
INNER JOIN {{ ref('fct_orders') }} AS o ON i.order_id = o.order_id
LEFT JOIN {{ ref('dim_product') }} AS p
    ON i.product_sk = p.product_sk AND p.is_inferred IS FALSE
WHERE o.order_status NOT IN ('canceled', 'unavailable')
GROUP BY 1
