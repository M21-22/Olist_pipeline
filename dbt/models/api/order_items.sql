-- Order detail endpoint uses this item-level contract. Source text comments,
-- customer identifiers and internal processing metadata are never exposed.
SELECT
    i.order_id,
    i.order_item_id,
    i.product_id,
    i.seller_id,
    p.product_category_name AS category_name,
    p.product_category_name_english AS category_name_english,
    i.price,
    i.freight_value,
    i.gross_item_amount,
    i.shipping_limit_at
FROM {{ ref('fct_order_items') }} AS i
LEFT JOIN {{ ref('dim_product') }} AS p
    ON i.product_sk = p.product_sk AND p.is_inferred IS FALSE
