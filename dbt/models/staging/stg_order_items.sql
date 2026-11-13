{{ config(materialized='table') }}

WITH ranked AS (
    SELECT
        src.*,
        ROW_NUMBER() OVER (
            PARTITION BY src.item_pk
            ORDER BY
                src.updated_at DESC NULLS LAST,
                registry.certified_at DESC NULLS LAST,
                src._batch_id DESC,
                src._row_hash DESC
        ) AS row_num
    FROM {{ source('raw', 'order_items') }} AS src
    INNER JOIN {{ source('audit', 'batch_registry') }} AS registry
        ON
            src._batch_id = registry.batch_id
            AND registry.table_name = 'order_items'
            AND registry.status = 'CERTIFIED'
)

SELECT
    src.item_pk,
    src.order_id,
    src.order_item_id,
    src.product_id,
    src.seller_id,
    src.shipping_limit_date,
    src.price,
    src.freight_value,
    src.updated_at
FROM ranked AS src
WHERE
    src.row_num = 1
    AND NOT EXISTS (
        SELECT 1
        FROM {{ source('audit', 'deleted_keys') }} AS deleted
        WHERE
            deleted.source_table = 'order_items'
            AND deleted.business_key = src.item_pk::TEXT
    )
