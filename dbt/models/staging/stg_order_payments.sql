{{ config(materialized='table') }}

WITH ranked AS (
    SELECT
        src.*,
        ROW_NUMBER() OVER (
            PARTITION BY src.order_id, src.payment_sequential
            ORDER BY
                src.updated_at DESC NULLS LAST,
                registry.certified_at DESC NULLS LAST,
                src._batch_id DESC,
                src._row_hash DESC
        ) AS row_num
    FROM {{ source('raw', 'order_payments') }} AS src
    INNER JOIN {{ source('audit', 'batch_registry') }} AS registry
        ON
            src._batch_id = registry.batch_id
            AND registry.table_name = 'order_payments'
            AND registry.status = 'CERTIFIED'
)

SELECT
    src.order_id,
    src.payment_sequential,
    src.payment_installments,
    src.payment_value,
    src.updated_at,
    LOWER(TRIM(payment_type)) AS payment_type
FROM ranked AS src
WHERE
    src.row_num = 1
    AND NOT EXISTS (
        SELECT 1
        FROM {{ source('audit', 'deleted_keys') }} AS deleted
        WHERE
            deleted.source_table = 'order_payments'
            AND deleted.business_key = JSONB_BUILD_ARRAY(
                src.order_id,
                src.payment_sequential
            )::TEXT
    )
