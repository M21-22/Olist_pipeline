-- Every published valid order should have exactly one API order row.
WITH actual AS (
    SELECT order_id, COUNT(*) AS n
    FROM {{ ref('orders') }}
    GROUP BY order_id
),
missing AS (
    SELECT o.order_id
    FROM {{ ref('fct_orders') }} AS o
    LEFT JOIN actual AS a ON o.order_id = a.order_id
    WHERE a.order_id IS NULL OR a.n <> 1
),
extra AS (
    SELECT a.order_id
    FROM actual AS a
    LEFT JOIN {{ ref('fct_orders') }} AS o ON a.order_id = o.order_id
    WHERE o.order_id IS NULL
)
SELECT order_id FROM missing
UNION ALL
SELECT order_id FROM extra
