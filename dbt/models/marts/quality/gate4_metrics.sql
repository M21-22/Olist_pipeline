{{ config(materialized='table') }}

-- Source checkpoints also move on zero-row batches and support historical replay.
WITH mutable_tables AS (
    SELECT table_name FROM (VALUES
        ('customers'), ('products'), ('sellers'), ('orders'),
        ('order_items'), ('order_payments'), ('order_reviews')
    ) AS inputs(table_name)
),
freshness AS (
    SELECT
        input.table_name,
        EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - newest.logical_interval_end)) / 60
            AS age_minutes,
        newest.expected_rows,
        newest.logical_interval_end
    FROM mutable_tables AS input
    LEFT JOIN LATERAL (
        SELECT landed.logical_interval_end, registry.expected_rows
        FROM {{ source('audit', 'batch_registry') }} AS registry
        JOIN audit.landed_batches AS landed USING (batch_id)
        JOIN audit.dbt_staging_processed_batches AS processed
          ON processed.batch_id = registry.batch_id
          AND processed.model_name = 'stg_' || input.table_name
        WHERE registry.table_name = input.table_name
          AND registry.status = 'CERTIFIED'
          AND landed.extraction_mode = 'incremental'
        ORDER BY landed.logical_interval_end DESC
        LIMIT 1
    ) AS newest ON TRUE
),
mart_window AS (
    SELECT CASE WHEN COUNT(logical_interval_end) = COUNT(*)
        THEN MIN(logical_interval_end) END AS logical_interval_end
    FROM freshness
),
order_metrics AS (
    SELECT
        COUNT(*) AS row_count,
        COUNT(*) FILTER (WHERE order_status = 'canceled')::numeric
            / NULLIF(COUNT(*), 0) AS cancellation_rate,
        COUNT(*) FILTER (WHERE customer_sk = MD5('__olist_unknown_customer__'))::numeric
            / NULLIF(COUNT(*), 0) AS unknown_customer_rate,
        COUNT(*) FILTER (WHERE delivered_to_customer_at IS NULL AND order_status = 'delivered')::numeric
            / NULLIF(COUNT(*) FILTER (WHERE order_status = 'delivered'), 0) AS missing_delivered_rate,
        COUNT(*) FILTER (
            WHERE item_count > 0 AND payment_count > 0
                AND item_price_total + freight_total <> payment_total
        )::numeric / NULLIF(COUNT(*) FILTER (WHERE item_count > 0 AND payment_count > 0), 0)
            AS settlement_difference_rate
    FROM {{ ref('fct_orders') }}
),
payment_metrics AS (
    SELECT COUNT(*) AS row_count, AVG(payment_value) AS mean_payment
    FROM {{ ref('fct_payments') }}
)
SELECT 'checkpoint_age_minutes'::text AS metric_name, table_name::text AS object_name,
       age_minutes::double precision AS metric_value, logical_interval_end AS source_interval_end
FROM freshness
UNION ALL SELECT 'batch_rows', table_name, expected_rows, logical_interval_end FROM freshness
UNION ALL
SELECT metrics.*, mart_window.logical_interval_end
FROM (
    SELECT 'mart_rows' AS metric_name, 'fct_orders' AS object_name, row_count AS metric_value FROM order_metrics
    UNION ALL SELECT 'mart_rows', 'fct_payments', row_count FROM payment_metrics
    UNION ALL SELECT 'mart_rows', 'fct_order_items', COUNT(*) FROM {{ ref('fct_order_items') }}
    UNION ALL SELECT 'mart_rows', 'fct_reviews', COUNT(*) FROM {{ ref('fct_reviews') }}
    UNION ALL SELECT 'mart_rows', 'mart_daily_kpis', COUNT(*) FROM {{ ref('mart_daily_kpis') }}
    UNION ALL SELECT 'mart_rows', 'mart_seller_delivery_sla', COUNT(*) FROM {{ ref('mart_seller_delivery_sla') }}
    UNION ALL SELECT 'cancellation_rate', 'fct_orders', cancellation_rate FROM order_metrics
    UNION ALL SELECT 'unknown_customer_rate', 'fct_orders', unknown_customer_rate FROM order_metrics
    UNION ALL SELECT 'missing_delivered_rate', 'fct_orders', missing_delivered_rate FROM order_metrics
    UNION ALL SELECT 'settlement_difference_rate', 'fct_orders', settlement_difference_rate FROM order_metrics
    UNION ALL SELECT 'mean_payment', 'fct_payments', mean_payment FROM payment_metrics
) AS metrics
CROSS JOIN mart_window
