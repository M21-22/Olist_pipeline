{{ config(
    unique_key='item_pk',
    pre_hook=[
        "{{ staging_lock() }}",
        "{{ staging_prepare('order_items') }}",
        "{{ staging_capture_pending('order_items') }}",
        "{{ staging_delete_tombstoned('order_items', ['target.item_pk']) }}"
    ],
    post_hook=[
        "{{ staging_mark_processed('order_items') }}",
        "{{ staging_finalize() }}"
    ]
) }}

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
    {{ staging_incremental_key_filter('order_items', ['item_pk']) }}
)

SELECT
    src._batch_id AS source_batch_id,
    {{ staging_is_valid('order_items', 'src') }} AS is_valid,
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
            AND deleted.business_key = {{ encode_business_key(["src.item_pk"]) }}
    )
