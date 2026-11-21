WITH versions AS (
    SELECT
        customer_unique_id,
        valid_from,
        valid_to,
        LEAD(valid_from) OVER (
            PARTITION BY customer_unique_id
            ORDER BY valid_from, customer_sk
        ) AS next_valid_from
    FROM {{ ref('dim_customer') }}
    WHERE NOT is_inferred
)
SELECT *
FROM versions
WHERE
    valid_from IS NULL
    OR (valid_to IS NOT NULL AND valid_to <= valid_from)
    OR (next_valid_from IS NOT NULL AND (
        valid_to IS NULL OR valid_to > next_valid_from
    ))

UNION ALL

SELECT
    customer_unique_id,
    NULL::timestamp with time zone AS valid_from,
    NULL::timestamp with time zone AS valid_to,
    NULL::timestamp with time zone AS next_valid_from
FROM {{ ref('dim_customer') }}
WHERE is_inferred IS FALSE
GROUP BY customer_unique_id
HAVING COUNT(*) FILTER (WHERE is_current IS TRUE) <> 1
