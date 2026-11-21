-- Current seller attributes (Type 1); changing a seller location does not create history.
WITH known_sellers AS (
    SELECT
        MD5('seller:' || seller_id)::text AS seller_sk,
        seller_id::text AS seller_id,
        seller_zip_code_prefix::text AS seller_zip_code_prefix,
        seller_city::text AS seller_city,
        seller_state::text AS seller_state,
        updated_at::timestamptz AS source_updated_at,
        source_batch_id::text AS source_batch_id,
        FALSE AS is_inferred
    FROM {{ ref('stg_sellers') }}
    WHERE is_valid IS TRUE
),
unknown_member AS (
    SELECT
        MD5('__olist_unknown_seller__')::text AS seller_sk,
        '__unknown__'::text AS seller_id,
        NULL::text AS seller_zip_code_prefix,
        'Unknown'::text AS seller_city,
        NULL::text AS seller_state,
        NULL::timestamptz AS source_updated_at,
        NULL::text AS source_batch_id,
        TRUE AS is_inferred
)
SELECT * FROM known_sellers
UNION ALL
SELECT * FROM unknown_member
