-- Calendar grain: one row per UTC purchase date, including the unknown date key.
-- GMV excludes canceled/unavailable orders and freight; payments are reported separately.
-- Review score is averaged per order first, preventing repeat reviews from overweighting it.
WITH reviews_per_order AS (
    SELECT order_id, AVG(review_score::numeric) AS avg_review_score
    FROM {{ ref('fct_reviews') }}
    WHERE review_score IS NOT NULL
    GROUP BY order_id
),
orders_enriched AS (
    SELECT
        o.purchase_date_sk,
        o.order_id,
        o.order_status,
        o.item_count,
        o.item_price_total,
        o.freight_total,
        o.payment_total,
        o.payment_count,
        r.avg_review_score
    FROM {{ ref('fct_orders') }} AS o
    LEFT JOIN reviews_per_order AS r ON o.order_id = r.order_id
)
SELECT
    o.purchase_date_sk,
    d.calendar_date AS purchase_date,
    COUNT(*)::bigint AS order_count,
    COUNT(*) FILTER (WHERE o.order_status = 'delivered')::bigint AS delivered_order_count,
    COUNT(*) FILTER (WHERE o.order_status = 'canceled')::bigint AS canceled_order_count,
    (COUNT(*) FILTER (WHERE o.order_status = 'canceled')::numeric
        / NULLIF(COUNT(*), 0))::numeric(12, 6) AS cancellation_rate,
    COALESCE(SUM(CASE
        WHEN o.order_status NOT IN ('canceled', 'unavailable')
            THEN o.item_price_total ELSE 0 END), 0)::numeric(18, 2) AS gmv,
    COALESCE(SUM(o.item_price_total), 0)::numeric(18, 2) AS item_price_total,
    COALESCE(SUM(o.freight_total), 0)::numeric(18, 2) AS freight_total,
    COALESCE(SUM(o.payment_total), 0)::numeric(18, 2) AS payment_total,
    COALESCE(SUM(o.item_count), 0)::bigint AS item_count,
    COALESCE(SUM(o.payment_count), 0)::bigint AS payment_count,
    COUNT(o.avg_review_score)::bigint AS reviewed_order_count,
    AVG(o.avg_review_score)::numeric(8, 4) AS avg_review_score
FROM orders_enriched AS o
LEFT JOIN {{ ref('dim_date') }} AS d ON o.purchase_date_sk = d.date_sk
GROUP BY o.purchase_date_sk, d.calendar_date
