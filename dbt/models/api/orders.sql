-- Stable order lookup/list contract. Customer geography is the currently
-- observed location, not a reconstructed location at historical purchase time.
WITH review_summary AS (
    SELECT
        order_id,
        AVG(review_score::numeric)::numeric(8, 4) AS average_review_score,
        COUNT(*) FILTER (WHERE review_score IS NOT NULL)::bigint AS scored_review_count
    FROM {{ ref('fct_reviews') }}
    GROUP BY order_id
)
SELECT
    o.order_id,
    o.order_status,
    o.purchased_at,
    o.approved_at,
    o.delivered_to_carrier_at,
    o.delivered_to_customer_at,
    o.estimated_delivery_at,
    o.delivery_delay_days,
    o.item_count,
    o.item_price_total,
    o.freight_total,
    o.payment_count,
    o.payment_total,
    o.purchase_date_sk,
    c.customer_state AS customer_state,
    r.average_review_score,
    COALESCE(r.scored_review_count, 0)::bigint AS scored_review_count
FROM {{ ref('fct_orders') }} AS o
LEFT JOIN {{ ref('dim_customer') }} AS c
    ON o.customer_sk = c.customer_sk AND c.is_inferred IS FALSE
LEFT JOIN review_summary AS r ON o.order_id = r.order_id
