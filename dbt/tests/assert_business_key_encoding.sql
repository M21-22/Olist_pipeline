{{ config(tags=['staging_contract']) }}
-- Singular dbt test: must return zero rows. Matches Python encode_key for
-- project business keys (non-null ASCII identifiers and integer components).
WITH cases AS (
    SELECT 'single_text' AS case_name,
           {{ encode_business_key(["'order-1'"]) }} AS actual,
           'order-1'::text AS expected
    UNION ALL
    SELECT 'single_integer',
           {{ encode_business_key(['123::bigint']) }},
           '123'::text
    UNION ALL
    SELECT 'composite',
           {{ encode_business_key(["'order-1'", '2::integer']) }},
           '["order-1", 2]'::text
    UNION ALL
    SELECT 'composite_special_chars',
           {{ encode_business_key(["$v$ord\"x$v$", '7::integer']) }},
           $expected$["ord\"x", 7]$expected$::text
)
SELECT * FROM cases WHERE actual IS DISTINCT FROM expected
