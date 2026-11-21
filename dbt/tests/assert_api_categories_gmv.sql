-- Category totals reconcile to item-level GMV under the same status policy.
WITH expected AS (
    SELECT COALESCE(SUM(i.price), 0)::numeric(18, 2) AS gmv
    FROM {{ ref('fct_order_items') }} AS i
    INNER JOIN {{ ref('fct_orders') }} AS o ON i.order_id = o.order_id
    WHERE o.order_status NOT IN ('canceled', 'unavailable')
),
actual AS (
    SELECT COALESCE(SUM(gmv), 0)::numeric(18, 2) AS gmv
    FROM {{ ref('categories_summary') }}
)
SELECT actual.gmv, expected.gmv AS expected_gmv
FROM actual CROSS JOIN expected
WHERE actual.gmv IS DISTINCT FROM expected.gmv
