{{ config(materialized='table') }}

WITH ranked AS (
    SELECT
        src.*,
        ROW_NUMBER() OVER (
            PARTITION BY src.review_pk
            ORDER BY
                src.updated_at DESC NULLS LAST,
                registry.certified_at DESC NULLS LAST,
                src._batch_id DESC,
                src._row_hash DESC
        ) AS row_num
    FROM {{ source('raw', 'order_reviews') }} AS src
    INNER JOIN {{ source('audit', 'batch_registry') }} AS registry
        ON
            src._batch_id = registry.batch_id
            AND registry.table_name = 'order_reviews'
            AND registry.status = 'CERTIFIED'
)

SELECT
    src.review_pk,
    src.review_id,
    src.order_id,
    src.review_score,
    src.review_creation_date,
    src.review_answer_timestamp,
    src.updated_at,
    NULLIF(TRIM(review_comment_title), '') AS review_comment_title,
    NULLIF(TRIM(review_comment_message), '') AS review_comment_message
FROM ranked AS src
WHERE
    src.row_num = 1
    AND NOT EXISTS (
        SELECT 1
        FROM {{ source('audit', 'deleted_keys') }} AS deleted
        WHERE
            deleted.source_table = 'order_reviews'
            AND deleted.business_key = src.review_pk::TEXT
    )
