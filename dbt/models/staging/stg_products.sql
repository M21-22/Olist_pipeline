{{ config(
    unique_key='product_id',
    pre_hook=[
        "{{ staging_lock() }}",
        "{{ staging_prepare('products') }}",
        "{{ staging_capture_pending('products') }}",
        "{{ staging_delete_tombstoned('products', ['target.product_id']) }}"
    ],
    post_hook=[
        "{{ staging_mark_processed('products') }}",
        "{{ staging_finalize() }}"
    ]
) }}

WITH ranked AS (
    SELECT
        src.*,
        ROW_NUMBER() OVER (
            PARTITION BY src.product_id
            ORDER BY
                {{ staging_version_order('src', 'registry') }}
        ) AS row_num
    FROM {{ source('raw', 'products') }} AS src
    INNER JOIN {{ source('audit', 'batch_registry') }} AS registry
        ON
            src._batch_id = registry.batch_id
            AND registry.table_name = 'products'
            AND registry.status = 'CERTIFIED'
    {{ staging_incremental_key_filter('products', ['product_id']) }}
    WHERE ({{ staging_captured_version('registry') }})
)

SELECT
    src._batch_id AS source_batch_id,
    {{ staging_is_valid('products', 'src') }} AS is_valid,
    src.product_id,
    src.product_name_lenght,
    src.product_description_lenght,
    src.product_photos_qty,
    src.product_weight_g,
    src.product_length_cm,
    src.product_height_cm,
    src.product_width_cm,
    src.updated_at,
    LOWER(TRIM(product_category_name)) AS product_category_name
FROM ranked AS src
WHERE
    src.row_num = 1
    AND NOT EXISTS (
        SELECT 1
        FROM {{ source('audit', 'deleted_keys') }} AS deleted
        WHERE
            deleted.source_table = 'products'
            AND deleted.business_key = {{ encode_business_key(["src.product_id"]) }}
    )
