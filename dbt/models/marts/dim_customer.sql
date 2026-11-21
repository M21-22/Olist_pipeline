-- One customer version per run of matching purchase-observed geography.
WITH purchase_addresses AS (
    SELECT DISTINCT ON (c.customer_unique_id, o.order_purchase_timestamp)
        c.customer_unique_id,
        c.customer_id AS representative_customer_id,
        c.customer_zip_code_prefix,
        c.customer_city,
        c.customer_state,
        o.order_purchase_timestamp AS valid_from
    FROM {{ ref('stg_orders') }} AS o
    JOIN {{ ref('stg_customers') }} AS c
      ON o.customer_id = c.customer_id AND c.is_valid IS TRUE
    WHERE o.is_valid IS TRUE
      AND o.order_purchase_timestamp IS NOT NULL
      AND c.customer_unique_id IS NOT NULL
    -- A stable order key breaks ties between purchases at the same instant.
    ORDER BY c.customer_unique_id, o.order_purchase_timestamp, o.order_id
),
address_changes AS (
    SELECT *,
        ROW(customer_zip_code_prefix, customer_city, customer_state)
        IS DISTINCT FROM ROW(
            LAG(customer_zip_code_prefix) OVER timeline,
            LAG(customer_city) OVER timeline,
            LAG(customer_state) OVER timeline
        ) AS address_changed,
        ROW_NUMBER() OVER timeline AS purchase_number
    FROM purchase_addresses
    WINDOW timeline AS (PARTITION BY customer_unique_id ORDER BY valid_from)
),
version_ranges AS (
    SELECT *,
        LEAD(valid_from) OVER (
            PARTITION BY customer_unique_id ORDER BY valid_from
        ) AS valid_to
    FROM address_changes
    WHERE purchase_number = 1 OR address_changed
),
observed_versions AS (
    SELECT
        MD5(JSONB_BUILD_ARRAY(customer_unique_id, EXTRACT(EPOCH FROM valid_from))::text) AS customer_sk,
        customer_unique_id,
        representative_customer_id,
        customer_zip_code_prefix,
        customer_city,
        customer_state,
        valid_from,
        valid_to,
        valid_to IS NULL AS is_current,
        FALSE AS is_inferred
    FROM version_ranges
),
unknown_member AS (
    SELECT
        MD5('__olist_unknown_customer__')::text AS customer_sk,
        '__unknown__'::text AS customer_unique_id,
        NULL::text AS representative_customer_id,
        NULL::text AS customer_zip_code_prefix,
        'Unknown'::text AS customer_city,
        NULL::text AS customer_state,
        NULL::timestamptz AS valid_from,
        NULL::timestamptz AS valid_to,
        TRUE AS is_current,
        TRUE AS is_inferred
)
SELECT * FROM observed_versions
UNION ALL
SELECT * FROM unknown_member
