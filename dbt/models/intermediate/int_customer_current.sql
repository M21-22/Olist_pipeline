{{ config(materialized='view', schema='marts') }}

WITH ranked AS (
    SELECT
        customer_unique_id,
        customer_id,
        customer_zip_code_prefix,
        customer_city,
        customer_state,
        updated_at,
        ROW_NUMBER() OVER (
            PARTITION BY customer_unique_id
            ORDER BY updated_at DESC NULLS LAST, customer_id DESC
        ) AS row_num
    FROM {{ ref('stg_customers') }}
    WHERE is_valid = TRUE AND customer_unique_id IS NOT NULL
)

SELECT
    customer_unique_id,
    customer_id AS representative_customer_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state,
    updated_at
FROM ranked
WHERE row_num = 1
