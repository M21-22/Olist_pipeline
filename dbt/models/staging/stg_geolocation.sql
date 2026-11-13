{{ config(materialized='table') }}

WITH latest_snapshot AS (
    SELECT landed.batch_id
    FROM {{ source('audit', 'batch_registry') }} AS registry
    INNER JOIN {{ source('audit', 'landed_batches') }} AS landed
        ON registry.batch_id = landed.batch_id
    WHERE
        registry.table_name = 'geolocation'
        AND registry.status = 'CERTIFIED'
        AND landed.extraction_mode = 'snapshot'
    ORDER BY registry.certified_at DESC NULLS LAST, landed.batch_id DESC
    LIMIT 1
),

ranked AS (
    SELECT
        src.*,
        ROW_NUMBER() OVER (
            PARTITION BY src.geolocation_pk
            ORDER BY
                registry.certified_at DESC NULLS LAST,
                src._batch_id DESC,
                src._row_hash DESC
        ) AS row_num
    FROM {{ source('raw', 'geolocation') }} AS src
    INNER JOIN latest_snapshot AS latest
        ON src._batch_id = latest.batch_id
    INNER JOIN {{ source('audit', 'batch_registry') }} AS registry
        ON src._batch_id = registry.batch_id
)

SELECT
    src.geolocation_pk,
    src.geolocation_zip_code_prefix,
    src.geolocation_lat,
    src.geolocation_lng,
    INITCAP(TRIM(geolocation_city)) AS geolocation_city,
    UPPER(TRIM(geolocation_state)) AS geolocation_state
FROM ranked AS src
WHERE src.row_num = 1
