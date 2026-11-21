SELECT
    purchase_date_sk,
    purchase_date,
    order_count,
    delivered_order_count,
    canceled_order_count,
    cancellation_rate,
    gmv,
    item_price_total,
    freight_total,
    payment_total,
    item_count,
    payment_count,
    reviewed_order_count,
    avg_review_score
FROM {{ ref('mart_daily_kpis') }}
