-- The daily mart must represent every fact order exactly once, with consistent sums.
WITH expected AS (
    SELECT
        purchase_date_sk,
        COUNT(*)::bigint AS order_count,
        COUNT(*) FILTER (WHERE order_status = 'canceled')::bigint AS canceled_order_count,
        SUM(CASE WHEN order_status NOT IN ('canceled', 'unavailable')
             THEN item_price_total ELSE 0 END)::numeric(18, 2) AS gmv,
        SUM(payment_total)::numeric(18, 2) AS payment_total
    FROM {{ ref('fct_orders') }}
    GROUP BY purchase_date_sk
)
SELECT
    COALESCE(e.purchase_date_sk, actual.purchase_date_sk) AS purchase_date_sk
FROM expected AS e
FULL OUTER JOIN {{ ref('mart_daily_kpis') }} AS actual
  ON e.purchase_date_sk = actual.purchase_date_sk
WHERE e.purchase_date_sk IS NULL OR actual.purchase_date_sk IS NULL
   OR e.order_count IS DISTINCT FROM actual.order_count
   OR e.canceled_order_count IS DISTINCT FROM actual.canceled_order_count
   OR e.gmv IS DISTINCT FROM actual.gmv
   OR e.payment_total IS DISTINCT FROM actual.payment_total
   OR actual.cancellation_rate IS DISTINCT FROM
      (e.canceled_order_count::numeric / NULLIF(e.order_count, 0))::numeric(12, 6)
   OR actual.reviewed_order_count > actual.order_count
