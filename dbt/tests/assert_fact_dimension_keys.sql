-- Unknown members are valid dimension references, not missing foreign keys.
WITH failures AS (
    SELECT 'orders_customer' AS relation, order_id AS business_key
    FROM {{ ref('fct_orders') }} f LEFT JOIN {{ ref('dim_customer') }} d USING(customer_sk)
    WHERE d.customer_sk IS NULL
    UNION ALL
    SELECT 'orders_geography', order_id
    FROM {{ ref('fct_orders') }} f LEFT JOIN {{ ref('dim_geography') }} d
      ON f.customer_geography_sk = d.geography_sk
    WHERE d.geography_sk IS NULL
    UNION ALL
    SELECT 'items_product', item_pk::text
    FROM {{ ref('fct_order_items') }} f LEFT JOIN {{ ref('dim_product') }} d USING(product_sk)
    WHERE d.product_sk IS NULL
    UNION ALL
    SELECT 'items_seller', item_pk::text
    FROM {{ ref('fct_order_items') }} f LEFT JOIN {{ ref('dim_seller') }} d USING(seller_sk)
    WHERE d.seller_sk IS NULL
    UNION ALL
    SELECT 'items_geography', item_pk::text
    FROM {{ ref('fct_order_items') }} f LEFT JOIN {{ ref('dim_geography') }} d
      ON f.seller_geography_sk = d.geography_sk
    WHERE d.geography_sk IS NULL
    UNION ALL
    SELECT 'items_customer', item_pk::text
    FROM {{ ref('fct_order_items') }} f LEFT JOIN {{ ref('dim_customer') }} d USING(customer_sk)
    WHERE d.customer_sk IS NULL
    UNION ALL
    SELECT 'payments_customer', order_id || ':' || payment_sequential::text
    FROM {{ ref('fct_payments') }} f LEFT JOIN {{ ref('dim_customer') }} d USING(customer_sk)
    WHERE d.customer_sk IS NULL
    UNION ALL
    SELECT 'reviews_customer', review_pk::text
    FROM {{ ref('fct_reviews') }} f LEFT JOIN {{ ref('dim_customer') }} d USING(customer_sk)
    WHERE d.customer_sk IS NULL
    UNION ALL
    SELECT 'orders_date', order_id
    FROM {{ ref('fct_orders') }} f LEFT JOIN {{ ref('dim_date') }} d
      ON f.purchase_date_sk = d.date_sk
    WHERE d.date_sk IS NULL
    UNION ALL
    SELECT 'items_date', item_pk::text
    FROM {{ ref('fct_order_items') }} f LEFT JOIN {{ ref('dim_date') }} d
      ON f.purchase_date_sk = d.date_sk
    WHERE d.date_sk IS NULL
    UNION ALL
    SELECT 'payments_date', order_id || ':' || payment_sequential::text
    FROM {{ ref('fct_payments') }} f LEFT JOIN {{ ref('dim_date') }} d
      ON f.purchase_date_sk = d.date_sk
    WHERE d.date_sk IS NULL
    UNION ALL
    SELECT 'reviews_date', review_pk::text
    FROM {{ ref('fct_reviews') }} f LEFT JOIN {{ ref('dim_date') }} d
      ON f.review_date_sk = d.date_sk
    WHERE d.date_sk IS NULL
)
SELECT * FROM failures
