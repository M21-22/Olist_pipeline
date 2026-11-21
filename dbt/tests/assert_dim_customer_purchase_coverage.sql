-- Every valid purchase with a valid identified customer must resolve to exactly
-- one purchase-effective, non-inferred customer dimension version.
WITH eligible_purchases AS (
    SELECT
        o.order_id,
        c.customer_unique_id,
        o.order_purchase_timestamp AS purchased_at
    FROM {{ ref('stg_orders') }} AS o
    JOIN {{ ref('stg_customers') }} AS c
        ON o.customer_id = c.customer_id
        AND c.is_valid IS TRUE
    WHERE o.is_valid IS TRUE
      AND o.order_purchase_timestamp IS NOT NULL
      AND c.customer_unique_id IS NOT NULL
),
matched AS (
    SELECT
        p.order_id,
        p.customer_unique_id,
        COUNT(d.customer_sk) AS matching_versions
    FROM eligible_purchases AS p
    LEFT JOIN {{ ref('dim_customer') }} AS d
        ON p.customer_unique_id = d.customer_unique_id
        AND d.is_inferred IS FALSE
        AND p.purchased_at >= d.valid_from
        AND (p.purchased_at < d.valid_to OR d.valid_to IS NULL)
    GROUP BY p.order_id, p.customer_unique_id
)
SELECT *
FROM matched
WHERE matching_versions <> 1
