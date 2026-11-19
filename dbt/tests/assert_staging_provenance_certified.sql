{{ config(tags=['staging_contract']) }}
-- Every staged source version must come from a certified batch of that table.
{% set tables = ['customers','orders','order_items','order_payments','order_reviews','products','sellers','geolocation','product_category_translation'] %}
{% for table in tables %}
SELECT '{{ table }}' AS source_table, src.source_batch_id
FROM {{ ref('stg_' ~ table) }} AS src
LEFT JOIN {{ source('audit','batch_registry') }} AS b
  ON b.batch_id = src.source_batch_id
 AND b.table_name = '{{ table }}'
 AND b.status = 'CERTIFIED'
WHERE b.batch_id IS NULL
{% if not loop.last %}UNION ALL{% endif %}
{% endfor %}
