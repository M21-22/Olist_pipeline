WITH expected_orders AS (
    SELECT
        o.order_id,
        o.customer_sk AS actual_key,
        COALESCE(d.customer_sk, MD5('__olist_unknown_customer__')) AS expected_key
    FROM {{ ref('fct_orders') }} AS o
    LEFT JOIN {{ ref('dim_customer') }} AS d
      ON o.customer_unique_id = d.customer_unique_id
     AND d.is_inferred IS FALSE
     AND o.purchased_at >= d.valid_from
     AND (o.purchased_at < d.valid_to OR d.valid_to IS NULL)
)
SELECT 'fct_orders' AS model_name, order_id::text
FROM expected_orders
WHERE actual_key IS DISTINCT FROM expected_key

{% for model in ['fct_order_items', 'fct_payments', 'fct_reviews'] %}
UNION ALL
SELECT '{{ model }}', f.order_id::text
FROM {{ ref(model) }} AS f
LEFT JOIN expected_orders AS o ON f.order_id = o.order_id
WHERE f.customer_sk IS DISTINCT FROM COALESCE(o.expected_key, MD5('__olist_unknown_customer__'))
{% endfor %}
