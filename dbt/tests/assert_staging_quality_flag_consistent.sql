{{ config(tags=['staging_contract']) }}
-- A staged row's validity must agree with the shared canonical rule predicates.
-- Review the model's normalization before applying additional rules here.
{% set tables = ['customers','orders','order_items','order_payments','order_reviews','products','sellers','geolocation','product_category_translation'] %}
{% for table in tables %}
SELECT '{{ table }}' AS source_table, stg.source_batch_id
FROM {{ ref('stg_' ~ table) }} AS stg
WHERE stg.is_valid IS DISTINCT FROM ({{ staging_is_valid(table,'stg') }})
{% if not loop.last %}UNION ALL{% endif %}
{% endfor %}
