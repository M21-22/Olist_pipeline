{{ config(materialized='view', schema='staging') }}

{{ find_rejected_rows('customers', "stg.customer_id", "raw_src.customer_id = stg.customer_id") }}
UNION ALL
{{ find_rejected_rows('orders', "stg.order_id", "raw_src.order_id = stg.order_id") }}
UNION ALL
{{ find_rejected_rows('order_items', "stg.item_pk", "raw_src.item_pk = stg.item_pk") }}
UNION ALL
{{ find_rejected_rows('order_payments', encode_business_key(["stg.order_id", "stg.payment_sequential"]), "raw_src.order_id = stg.order_id AND raw_src.payment_sequential = stg.payment_sequential") }}
UNION ALL
{{ find_rejected_rows('order_reviews', "stg.review_pk", "raw_src.review_pk = stg.review_pk") }}
UNION ALL
{{ find_rejected_rows('products', "stg.product_id", "raw_src.product_id = stg.product_id") }}
UNION ALL
{{ find_rejected_rows('sellers', "stg.seller_id", "raw_src.seller_id = stg.seller_id") }}
UNION ALL
{{ find_rejected_rows('geolocation', "stg.geolocation_pk", "raw_src.geolocation_pk = stg.geolocation_pk") }}
UNION ALL
{{ find_rejected_rows('product_category_translation', "stg.product_category_name", "LOWER(TRIM(raw_src.product_category_name)) = stg.product_category_name") }}
