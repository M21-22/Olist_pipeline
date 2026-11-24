{{ config(
    unique_key='seller_id',
    pre_hook=[
        "{{ staging_lock() }}",
        "{{ staging_prepare('sellers') }}",
        "{{ staging_capture_pending('sellers') }}",
        "{{ staging_delete_tombstoned('sellers', ['target.seller_id']) }}"
    ],
    post_hook=[
        "{{ staging_mark_processed('sellers') }}",
        "{{ staging_finalize() }}"
    ]
) }}

WITH ranked AS (
    SELECT
        src.*,
        ROW_NUMBER() OVER (
            PARTITION BY src.seller_id
            ORDER BY
                {{ staging_version_order('src', 'registry') }}
        ) AS row_num
    FROM {{ source('raw', 'sellers') }} AS src
    INNER JOIN {{ source('audit', 'batch_registry') }} AS registry
        ON
            src._batch_id = registry.batch_id
            AND registry.table_name = 'sellers'
            AND registry.status = 'CERTIFIED'
    {{ staging_incremental_key_filter('sellers', ['seller_id']) }}
    WHERE ({{ staging_captured_version('registry') }})
)

SELECT
    src._batch_id AS source_batch_id,
    {{ staging_is_valid('sellers', 'src') }} AS is_valid,
    src.seller_id,
    src.seller_zip_code_prefix,
    src.updated_at,
    INITCAP(TRIM(seller_city)) AS seller_city,
    UPPER(TRIM(seller_state)) AS seller_state
FROM ranked AS src
WHERE
    src.row_num = 1
    AND NOT EXISTS (
        SELECT 1
        FROM {{ source('audit', 'deleted_keys') }} AS deleted
        WHERE
            deleted.source_table = 'sellers'
            AND deleted.business_key = {{ encode_business_key(["src.seller_id"]) }}
    )
