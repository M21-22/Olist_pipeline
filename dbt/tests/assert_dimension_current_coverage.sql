-- Compare both directions: a missing valid business key or an obsolete member fails.
-- Covers current-state (Type 1/reference) dimensions; customer is SCD2.
{% set dimensions = [
  {'model': 'dim_product', 'source': 'stg_products', 'source_key': 'product_id', 'dimension_key': 'product_id', 'predicate': 'is_valid IS TRUE'},
  {'model': 'dim_seller', 'source': 'stg_sellers', 'source_key': 'seller_id', 'dimension_key': 'seller_id', 'predicate': 'is_valid IS TRUE'},
  {'model': 'dim_geography', 'source': 'stg_geolocation', 'source_key': 'geolocation_zip_code_prefix', 'dimension_key': 'zip_code_prefix', 'predicate': 'is_valid IS TRUE AND geolocation_zip_code_prefix IS NOT NULL'}
] %}
{% for dim in dimensions %}
SELECT '{{ dim.model }}' AS dimension, 'missing_dimension_row' AS violation, missing.business_key
FROM (
    SELECT {{ dim.source_key }}::text AS business_key
    FROM {{ ref(dim.source) }}
    WHERE {{ dim.predicate }}
    EXCEPT
    SELECT {{ dim.dimension_key }}::text
    FROM {{ ref(dim.model) }}
    WHERE is_inferred IS FALSE
) AS missing
UNION ALL
SELECT '{{ dim.model }}', 'unexpected_dimension_row', unexpected.business_key
FROM (
    SELECT {{ dim.dimension_key }}::text AS business_key
    FROM {{ ref(dim.model) }}
    WHERE is_inferred IS FALSE
    EXCEPT
    SELECT {{ dim.source_key }}::text
    FROM {{ ref(dim.source) }}
    WHERE {{ dim.predicate }}
) AS unexpected
{% if not loop.last %}UNION ALL{% endif %}
{% endfor %}
