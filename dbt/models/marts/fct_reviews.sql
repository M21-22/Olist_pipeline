{{ config(
    materialized='incremental', incremental_strategy='merge',
    unique_key='review_pk', on_schema_change='fail',
    pre_hook="{{ fact_prune_invalid('stg_order_reviews', ['review_pk']) }}"
) }}

-- Review grain is review_pk (review_id is not guaranteed to be unique).
WITH projected AS (
    SELECT
        r.review_pk::bigint AS review_pk,
        MD5('review:' || r.review_pk::text)::text AS review_sk,
        r.review_id::text AS review_id,
        r.order_id::text AS order_id,
        r.review_score::integer AS review_score,
        r.review_creation_date AS review_created_at,
        r.review_answer_timestamp AS review_answered_at,
        r.review_comment_title::text AS review_comment_title,
        r.review_comment_message::text AS review_comment_message,
        COALESCE(dd.date_sk, 0)::integer AS review_date_sk,
        COALESCE(dc.customer_sk, MD5('__olist_unknown_customer__'))::text AS customer_sk,
        r.updated_at AS source_updated_at,
        r.source_batch_id::text AS source_batch_id
    FROM {{ ref('stg_order_reviews') }} AS r
    LEFT JOIN {{ ref('stg_orders') }} AS o
      ON r.order_id = o.order_id AND o.is_valid IS TRUE
    LEFT JOIN {{ ref('stg_customers') }} AS c
      ON o.customer_id = c.customer_id AND c.is_valid IS TRUE
    LEFT JOIN {{ ref('dim_customer') }} AS dc
      ON c.customer_unique_id = dc.customer_unique_id
      AND dc.is_inferred IS FALSE
      AND o.order_purchase_timestamp >= dc.valid_from
      AND (o.order_purchase_timestamp < dc.valid_to OR dc.valid_to IS NULL)
    LEFT JOIN {{ ref('dim_date') }} AS dd
      ON (r.review_creation_date AT TIME ZONE 'UTC')::date = dd.calendar_date
    WHERE r.is_valid IS TRUE
),
candidate AS (
    SELECT projected.*, MD5(TO_JSONB(projected)::text)::text AS row_fingerprint
    FROM projected
)
SELECT candidate.*
FROM candidate
{{ fact_changed_rows(['review_pk']) }}
