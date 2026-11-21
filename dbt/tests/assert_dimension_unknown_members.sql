-- All dimensions require exactly one well-formed inferred/unknown row.
-- Return model and violation so one consolidated test remains diagnosable.
{% set dimensions = [
  {'model': 'dim_customer', 'condition': "customer_unique_id = '__unknown__' AND is_current IS TRUE"},
  {'model': 'dim_product', 'condition': "product_id = '__unknown__' AND product_sk = MD5('__olist_unknown_product__')"},
  {'model': 'dim_seller', 'condition': "seller_id = '__unknown__' AND seller_sk = MD5('__olist_unknown_seller__')"},
  {'model': 'dim_geography', 'condition': "zip_code_prefix = '__unknown__' AND geography_sk = MD5('__olist_unknown_geography__') AND coordinate_samples = 0"},
  {'model': 'dim_date', 'condition': 'date_sk = 0 AND calendar_date IS NULL'}
] %}
{% for dim in dimensions %}
SELECT '{{ dim.model }}' AS dimension, 'unknown_count_or_attributes' AS violation
WHERE (
    SELECT COUNT(*)
    FROM {{ ref(dim.model) }}
    WHERE is_inferred IS TRUE AND {{ dim.condition }}
) <> 1
UNION ALL
SELECT '{{ dim.model }}', 'unexpected_inferred_row'
WHERE EXISTS (
    SELECT 1
    FROM {{ ref(dim.model) }}
    WHERE is_inferred IS TRUE AND NOT ({{ dim.condition }})
)
{% if not loop.last %}UNION ALL{% endif %}
{% endfor %}
