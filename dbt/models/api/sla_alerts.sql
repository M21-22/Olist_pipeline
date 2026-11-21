-- Alert-only projection uses the identical threshold logic as the SLA mart.
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
    delay_z_score
FROM {{ ref('mart_seller_delivery_sla') }}
WHERE is_sla_alert IS TRUE
