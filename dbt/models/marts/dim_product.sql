-- Type 1 dimension: each successful build reflects the latest valid product state.
-- Hash-based keys remain stable when descriptive attributes change.
WITH valid_products AS (
    SELECT
        product_id,
        product_category_name,
        product_name_lenght,
        product_description_lenght,
        product_photos_qty,
        product_weight_g,
        product_length_cm,
        product_height_cm,
        product_width_cm,
        updated_at,
        source_batch_id
    FROM {{ ref('stg_products') }}
    WHERE is_valid IS TRUE
),
valid_translations AS (
    SELECT
        product_category_name,
        product_category_name_english
    FROM {{ ref('stg_product_category_translation') }}
    WHERE is_valid IS TRUE
),
known_products AS (
    SELECT
        MD5('product:' || p.product_id)::text AS product_sk,
        p.product_id::text AS product_id,
        p.product_category_name::text AS product_category_name,
        t.product_category_name_english::text AS product_category_name_english,
        p.product_name_lenght::integer AS product_name_length,
        p.product_description_lenght::integer AS product_description_length,
        p.product_photos_qty::integer AS product_photos_qty,
        p.product_weight_g::integer AS product_weight_g,
        p.product_length_cm::integer AS product_length_cm,
        p.product_height_cm::integer AS product_height_cm,
        p.product_width_cm::integer AS product_width_cm,
        p.updated_at::timestamptz AS source_updated_at,
        p.source_batch_id::text AS source_batch_id,
        FALSE AS is_inferred
    FROM valid_products AS p
    LEFT JOIN valid_translations AS t
        ON p.product_category_name = t.product_category_name
),
unknown_member AS (
    SELECT
        MD5('__olist_unknown_product__')::text AS product_sk,
        '__unknown__'::text AS product_id,
        NULL::text AS product_category_name,
        NULL::text AS product_category_name_english,
        NULL::integer AS product_name_length,
        NULL::integer AS product_description_length,
        NULL::integer AS product_photos_qty,
        NULL::integer AS product_weight_g,
        NULL::integer AS product_length_cm,
        NULL::integer AS product_height_cm,
        NULL::integer AS product_width_cm,
        NULL::timestamptz AS source_updated_at,
        NULL::text AS source_batch_id,
        TRUE AS is_inferred
)
SELECT * FROM known_products
UNION ALL
SELECT * FROM unknown_member
