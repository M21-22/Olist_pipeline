{{ config(
    unique_key=['order_id', 'payment_sequential'],
    pre_hook=[
        "{{ staging_lock() }}",
        "{{ staging_prepare('order_payments') }}",
        "{{ staging_capture_pending('order_payments') }}",
        "{{ staging_delete_tombstoned('order_payments', ['target.order_id', 'target.payment_sequential']) }}"
    ],
    post_hook=[
        "{{ staging_mark_processed('order_payments') }}",
        "{{ staging_finalize() }}"
    ]
) }}

WITH ranked AS (
    SELECT
        src.*,
        ROW_NUMBER() OVER (
            PARTITION BY src.order_id, src.payment_sequential
            ORDER BY
                {{ staging_version_order('src', 'registry') }}
        ) AS row_num
    FROM {{ source('raw', 'order_payments') }} AS src
    INNER JOIN {{ source('audit', 'batch_registry') }} AS registry
        ON
            src._batch_id = registry.batch_id
            AND registry.table_name = 'order_payments'
            AND registry.status = 'CERTIFIED'
    {{ staging_incremental_key_filter('order_payments', ['order_id', 'payment_sequential']) }}
    WHERE ({{ staging_captured_version('registry') }})
)

SELECT
    src._batch_id AS source_batch_id,
    {{ staging_is_valid('order_payments', 'src') }} AS is_valid,
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
            AND deleted.business_key = {{ encode_business_key(["src.order_id", "src.payment_sequential"]) }}
    )
