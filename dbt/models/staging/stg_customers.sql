{{ config(
    unique_key='customer_id',
    pre_hook=[
        "{{ staging_lock() }}",
        "{{ staging_prepare('customers') }}",
        "{{ staging_capture_pending('customers') }}",
        "{{ staging_delete_tombstoned('customers', ['target.customer_id']) }}"
    ],
    post_hook=[
        "{{ staging_mark_processed('customers') }}",
        "{{ staging_finalize() }}"
    ]
) }}

WITH ranked AS (
    SELECT
        src.*,
        ROW_NUMBER() OVER (
            PARTITION BY src.customer_id
            ORDER BY
                src.updated_at DESC NULLS LAST,
                registry.certified_at DESC NULLS LAST,
                src._batch_id DESC,
                src._row_hash DESC
        ) AS row_num
    FROM {{ source('raw', 'customers') }} AS src
    INNER JOIN {{ source('audit', 'batch_registry') }} AS registry
        ON
            src._batch_id = registry.batch_id
            AND registry.table_name = 'customers'
            AND registry.status = 'CERTIFIED'
    {{ staging_incremental_key_filter('customers', ['customer_id']) }}
)

SELECT
    src._batch_id AS source_batch_id,
    {{ staging_is_valid('customers', 'src') }} AS is_valid,
    src.customer_id,
    src.customer_unique_id,
    src.customer_zip_code_prefix,
    src.updated_at,
    INITCAP(TRIM(customer_city)) AS customer_city,
    UPPER(TRIM(customer_state)) AS customer_state
FROM ranked AS src
WHERE
    src.row_num = 1
    AND NOT EXISTS (
        SELECT 1
        FROM {{ source('audit', 'deleted_keys') }} AS deleted
        WHERE
            deleted.source_table = 'customers'
            AND deleted.business_key = {{ encode_business_key(["src.customer_id"]) }}
    )
