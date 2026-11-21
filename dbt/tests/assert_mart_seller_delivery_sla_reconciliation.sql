-- Protect seller/order deduplication, SLA ratios and baseline eligibility.
WITH expected AS (
    SELECT
        i.seller_id,
        COALESCE(ds.seller_state, 'Unknown') AS seller_state,
        (o.delivered_to_customer_at AT TIME ZONE 'UTC')::date AS delivery_date,
        COUNT(*)::bigint AS delivered_order_count,
        COUNT(*) FILTER (WHERE o.delivery_delay_days > 0)::bigint AS late_order_count
    FROM (
        SELECT DISTINCT seller_id, order_id FROM {{ ref('fct_order_items') }}
    ) AS i
    INNER JOIN {{ ref('fct_orders') }} AS o ON i.order_id = o.order_id
    LEFT JOIN {{ ref('dim_seller') }} AS ds
      ON i.seller_id = ds.seller_id AND ds.is_inferred IS FALSE
    WHERE o.order_status = 'delivered'
      AND o.delivered_to_customer_at IS NOT NULL
      AND o.delivery_delay_days IS NOT NULL
    GROUP BY 1, 2, 3
)
SELECT
    COALESCE(e.seller_id, actual.seller_id) AS seller_id,
    COALESCE(e.delivery_date, actual.delivery_date) AS delivery_date
FROM expected AS e
FULL OUTER JOIN {{ ref('mart_seller_delivery_sla') }} AS actual
  ON e.seller_id = actual.seller_id
 AND e.seller_state = actual.seller_state
 AND e.delivery_date = actual.delivery_date
WHERE e.seller_id IS NULL OR actual.seller_id IS NULL
   OR e.delivered_order_count IS DISTINCT FROM actual.delivered_order_count
   OR e.late_order_count IS DISTINCT FROM actual.late_order_count
   OR actual.late_delivery_rate IS DISTINCT FROM
      (e.late_order_count::numeric / NULLIF(e.delivered_order_count, 0))::numeric(12, 6)
   OR actual.baseline_order_count < 0
   OR (actual.baseline_order_count < {{ var('seller_sla_min_baseline_orders', 10) | int }}
       AND actual.delay_z_score IS NOT NULL)
   OR (actual.is_sla_alert AND
       (actual.delay_z_score IS NULL
        OR actual.delay_z_score <= {{ var('seller_sla_z_threshold', 2.0) | float }}
        OR actual.avg_delay_days <= 0))

UNION ALL

SELECT
    seller_id,
    delivery_date
FROM {{ ref('mart_seller_delivery_sla') }}
GROUP BY seller_id, seller_state, delivery_date
HAVING COUNT(*) > 1
