-- Daily seller-by-current-region delivery performance.
-- Every (seller_id, order_id) is counted once, even when it has multiple items.
-- A positive z-score indicates slower deliveries relative to the previous 30
-- calendar days. The baseline excludes the current day to avoid leakage.
WITH seller_order_links AS (
    SELECT DISTINCT seller_id, order_id
    FROM {{ ref('fct_order_items') }}
),
delivered AS (
    SELECT
        links.seller_id,
        COALESCE(ds.seller_sk, MD5('__olist_unknown_seller__')) AS seller_sk,
        COALESCE(ds.seller_state, 'Unknown') AS seller_state,
        COALESCE(dg.geography_sk, MD5('__olist_unknown_geography__')) AS seller_geography_sk,
        (o.delivered_to_customer_at AT TIME ZONE 'UTC')::date AS delivery_date,
        o.delivery_delay_days::double precision AS delay_days
    FROM seller_order_links AS links
    INNER JOIN {{ ref('fct_orders') }} AS o ON links.order_id = o.order_id
    LEFT JOIN {{ ref('dim_seller') }} AS ds
      ON links.seller_id = ds.seller_id AND ds.is_inferred IS FALSE
    LEFT JOIN {{ ref('dim_geography') }} AS dg
      ON ds.seller_zip_code_prefix = dg.zip_code_prefix AND dg.is_inferred IS FALSE
    WHERE
        o.order_status = 'delivered'
        AND o.delivered_to_customer_at IS NOT NULL
        AND o.delivery_delay_days IS NOT NULL
),
daily AS (
    SELECT
        seller_id,
        seller_sk,
        seller_state,
        seller_geography_sk,
        delivery_date,
        COUNT(*)::bigint AS delivered_order_count,
        COUNT(*) FILTER (WHERE delay_days > 0)::bigint AS late_order_count,
        SUM(delay_days) AS delay_sum,
        SUM(delay_days * delay_days) AS delay_sq_sum,
        AVG(delay_days) AS avg_delay_days
    FROM delivered
    GROUP BY seller_id, seller_sk, seller_state, seller_geography_sk, delivery_date
),
prior_30_days AS (
    SELECT
        cur.seller_id,
        cur.seller_state,
        cur.delivery_date,
        COALESCE(SUM(prev.delivered_order_count), 0)::bigint AS baseline_order_count,
        SUM(prev.delay_sum) AS baseline_delay_sum,
        SUM(prev.delay_sq_sum) AS baseline_delay_sq_sum
    FROM daily AS cur
    LEFT JOIN daily AS prev
      ON cur.seller_id = prev.seller_id
     AND cur.seller_state = prev.seller_state
     AND prev.delivery_date >= cur.delivery_date - INTERVAL '30 days'
     AND prev.delivery_date < cur.delivery_date
    GROUP BY cur.seller_id, cur.seller_state, cur.delivery_date
),
metrics AS (
    SELECT
        cur.*,
        b.baseline_order_count,
        (b.baseline_delay_sum / NULLIF(b.baseline_order_count, 0))
            AS baseline_mean_delay_days,
        CASE WHEN b.baseline_order_count >= 2 THEN
            SQRT(GREATEST(0.0,
                (b.baseline_delay_sq_sum -
                 b.baseline_delay_sum * b.baseline_delay_sum / b.baseline_order_count)
                 / (b.baseline_order_count - 1)
            ))
        END AS baseline_stddev_delay_days
    FROM daily AS cur
    INNER JOIN prior_30_days AS b
      ON cur.seller_id = b.seller_id
     AND cur.seller_state = b.seller_state
     AND cur.delivery_date = b.delivery_date
),
scored AS (
    SELECT
        metrics.*,
        CASE
          WHEN baseline_order_count >= {{ var('seller_sla_min_baseline_orders', 10) | int }}
           AND baseline_stddev_delay_days > 0
          THEN (avg_delay_days - baseline_mean_delay_days) / baseline_stddev_delay_days
        END AS delay_z_score
    FROM metrics
)
SELECT
    seller_id,
    seller_sk,
    seller_state,
    seller_geography_sk,
    delivery_date,
    delivered_order_count,
    late_order_count,
    (late_order_count::numeric / NULLIF(delivered_order_count, 0))::numeric(12, 6)
        AS late_delivery_rate,
    avg_delay_days::numeric(14, 4) AS avg_delay_days,
    baseline_order_count,
    baseline_mean_delay_days::numeric(14, 4) AS baseline_mean_delay_days,
    baseline_stddev_delay_days::numeric(14, 4) AS baseline_stddev_delay_days,
    delay_z_score::numeric(14, 4) AS delay_z_score,
    (COALESCE(delay_z_score > {{ var('seller_sla_z_threshold', 2.0) | float }}, FALSE)
      AND avg_delay_days > 0) AS is_sla_alert
FROM scored
