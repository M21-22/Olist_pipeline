-- Type 1 attributes and lineage must reflect the latest valid staging winner.
SELECT 'dim_product' AS dimension, p.product_id AS business_key
FROM {{ ref('stg_products') }} AS p
LEFT JOIN {{ ref('dim_product') }} AS d ON p.product_id = d.product_id
WHERE p.is_valid IS TRUE AND (
    d.product_id IS NULL
    OR d.product_sk IS DISTINCT FROM MD5('product:' || p.product_id)
    OR d.product_category_name IS DISTINCT FROM p.product_category_name
    OR d.product_weight_g IS DISTINCT FROM p.product_weight_g
    OR d.source_updated_at IS DISTINCT FROM p.updated_at
    OR d.source_batch_id IS DISTINCT FROM p.source_batch_id
)
UNION ALL
SELECT 'dim_seller', s.seller_id
FROM {{ ref('stg_sellers') }} AS s
LEFT JOIN {{ ref('dim_seller') }} AS d ON s.seller_id = d.seller_id
WHERE s.is_valid IS TRUE AND (
    d.seller_id IS NULL
    OR d.seller_sk IS DISTINCT FROM MD5('seller:' || s.seller_id)
    OR d.seller_zip_code_prefix IS DISTINCT FROM s.seller_zip_code_prefix
    OR d.seller_city IS DISTINCT FROM s.seller_city
    OR d.seller_state IS DISTINCT FROM s.seller_state
    OR d.source_updated_at IS DISTINCT FROM s.updated_at
    OR d.source_batch_id IS DISTINCT FROM s.source_batch_id
    OR d.is_inferred IS DISTINCT FROM FALSE
)
