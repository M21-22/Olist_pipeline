{{ config(materialized='table') }}

WITH ranked AS (
    SELECT
        src.*,
        ROW_NUMBER() OVER (
            PARTITION BY src.order_id
            ORDER BY
                src.updated_at DESC NULLS LAST,
                registry.certified_at DESC NULLS LAST,
                src._batch_id DESC,
                src._row_hash DESC
        ) AS row_num
    FROM {{ source('raw', 'orders') }} AS src
    INNER JOIN {{ source('audit', 'batch_registry') }} AS registry
        ON
            src._batch_id = registry.batch_id
            AND registry.table_name = 'orders'
            AND registry.status = 'CERTIFIED'
)

SELECT
    src.order_id,
    src.customer_id,
    src.order_purchase_timestamp,
    src.order_approved_at,
    src.order_delivered_carrier_date,
    src.order_delivered_customer_date,
    src.order_estimated_delivery_date,
    src.updated_at,
    LOWER(TRIM(order_status)) AS order_status
FROM ranked AS src
WHERE
    src.row_num = 1
    AND NOT EXISTS (
        SELECT 1
        FROM {{ source('audit', 'deleted_keys') }} AS deleted
        WHERE
            deleted.source_table = 'orders'
            AND deleted.business_key = src.order_id::TEXT
    )
