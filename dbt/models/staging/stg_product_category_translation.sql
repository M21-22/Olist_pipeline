{{ config(materialized='table') }}

WITH latest_snapshot AS (
    SELECT landed.batch_id
    FROM {{ source('audit', 'batch_registry') }} AS registry
    INNER JOIN {{ source('audit', 'landed_batches') }} AS landed
        ON registry.batch_id = landed.batch_id
    WHERE
        registry.table_name = 'product_category_translation'
        AND registry.status = 'CERTIFIED'
        AND landed.extraction_mode = 'snapshot'
    ORDER BY registry.certified_at DESC NULLS LAST, landed.batch_id DESC
    LIMIT 1
),

ranked AS (
    SELECT
        src.*,
        ROW_NUMBER() OVER (
            PARTITION BY src.product_category_name
            ORDER BY
                registry.certified_at DESC NULLS LAST,
                src._batch_id DESC,
                src._row_hash DESC
        ) AS row_num
    FROM {{ source('raw', 'product_category_translation') }} AS src
    INNER JOIN latest_snapshot AS latest
        ON src._batch_id = latest.batch_id
    INNER JOIN {{ source('audit', 'batch_registry') }} AS registry
        ON src._batch_id = registry.batch_id
)

SELECT
    LOWER(TRIM(product_category_name)) AS product_category_name,
    TRIM(product_category_name_english) AS product_category_name_english
FROM ranked
WHERE row_num = 1
