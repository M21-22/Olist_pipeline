{{ config(
    unique_key='order_id',
    pre_hook=[
        "{{ staging_lock() }}",
        "{{ staging_prepare('orders') }}",
        "{{ staging_capture_pending('orders') }}",
        "{{ staging_delete_tombstoned('orders', ['target.order_id']) }}"
    ],
    post_hook=[
        "{{ staging_mark_processed('orders') }}",
        "{{ staging_finalize() }}"
    ]
) }}

WITH ranked AS (
    SELECT
        src.*,
        ROW_NUMBER() OVER (
            PARTITION BY src.order_id
            ORDER BY
                {{ staging_version_order('src', 'registry') }}
        ) AS row_num
    FROM {{ source('raw', 'orders') }} AS src
    INNER JOIN {{ source('audit', 'batch_registry') }} AS registry
        ON
            src._batch_id = registry.batch_id
            AND registry.table_name = 'orders'
            AND registry.status = 'CERTIFIED'
    {{ staging_incremental_key_filter('orders', ['order_id']) }}
    WHERE ({{ staging_captured_version('registry') }})
)

SELECT
    src._batch_id AS source_batch_id,
    {{ staging_is_valid('orders', 'src') }} AS is_valid,
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
            AND deleted.business_key = {{ encode_business_key(["src.order_id"]) }}
    )
