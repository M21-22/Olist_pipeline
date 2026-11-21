-- Seller region is current Type 1 attribution, not historical location.
SELECT
    seller_id,
    seller_state,
    delivery_date,
    delivered_order_count,
    late_order_count,
    late_delivery_rate,
    avg_delay_days,
    baseline_order_count,
    baseline_mean_delay_days,
    baseline_stddev_delay_days,
    delay_z_score,
    is_sla_alert
FROM {{ ref('mart_seller_delivery_sla') }}
