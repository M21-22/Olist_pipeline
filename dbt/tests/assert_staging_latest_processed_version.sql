{{ config(tags=['staging_contract']) }}

-- Check presence and provenance against the processed raw versions. A batch
-- certified after this model's cutoff belongs to the next staging run.
{% set keys = {
    'customers': ['customer_id'],
    'products': ['product_id'],
    'sellers': ['seller_id'],
    'orders': ['order_id'],
    'order_items': ['item_pk'],
    'order_payments': ['order_id', 'payment_sequential'],
    'order_reviews': ['review_pk']
} %}
{% for table, columns in keys.items() %}
{% set raw_keys = [] %}
{% set staging_keys = [] %}
{% for column in columns %}
    {% do raw_keys.append('r.' ~ column) %}
    {% do staging_keys.append('s.' ~ column) %}
{% endfor %}
(
    WITH expected AS (
        SELECT DISTINCT ON ({{ raw_keys | join(', ') }})
            {{ encode_business_key(raw_keys) }} AS business_key, r._batch_id, r.updated_at
        FROM {{ source('raw', table) }} AS r
        JOIN {{ source('audit', 'batch_registry') }} AS registry
          ON registry.batch_id = r._batch_id
          AND registry.table_name = '{{ table }}' AND registry.status = 'CERTIFIED'
        JOIN audit.dbt_staging_processed_batches AS processed
          ON processed.batch_id = registry.batch_id AND processed.model_name = 'stg_{{ table }}'
        WHERE NOT EXISTS (
            SELECT 1 FROM {{ source('audit', 'deleted_keys') }} AS deleted
            WHERE deleted.source_table = '{{ table }}'
              AND deleted.business_key = {{ encode_business_key(raw_keys) }}
        )
        ORDER BY {{ raw_keys | join(', ') }}, {{ staging_version_order('r', 'registry') }}
    ),
    actual AS (
        SELECT {{ encode_business_key(staging_keys) }} AS business_key, s.source_batch_id, s.updated_at
        FROM {{ ref('stg_' ~ table) }} AS s
    )
    SELECT '{{ table }}' AS source_table, COALESCE(expected.business_key, actual.business_key) AS business_key
    FROM expected
    FULL OUTER JOIN actual USING (business_key)
    WHERE expected._batch_id IS DISTINCT FROM actual.source_batch_id
       OR expected.updated_at IS DISTINCT FROM actual.updated_at
)
{% if not loop.last %}UNION ALL{% endif %}
{% endfor %}
