{# Quality checks are evaluated on CURRENT staged data, not all raw history.
   The same predicates feed the staging is_valid flag and rejection candidates.
   NULL handling for required identifiers remains the Gate 1 responsibility. #}
{% macro staging_quality_rules(table_name, a) %}
    {% set states = "('AC','AL','AP','AM','BA','CE','DF','ES','GO','MA','MT','MS','MG','PA','PB','PR','PE','PI','RJ','RN','RS','RO','RR','SC','SP','SE','TO')" %}
    {% if table_name == 'customers' %}
        {{ return({'invalid_customer_state': a ~ '.customer_state IS NOT NULL AND UPPER(TRIM(' ~ a ~ '.customer_state)) NOT IN ' ~ states}) }}
    {% elif table_name == 'orders' %}
        {{ return({
            'unknown_order_status': a ~ ".order_status IS NOT NULL AND LOWER(TRIM(" ~ a ~ ".order_status)) NOT IN ('created','approved','invoiced','processing','shipped','delivered','canceled','unavailable')",
            'approved_before_purchase': a ~ '.order_approved_at < ' ~ a ~ '.order_purchase_timestamp',
            'delivered_before_purchase': a ~ '.order_delivered_customer_date < ' ~ a ~ '.order_purchase_timestamp',
            'delivered_before_carrier': a ~ '.order_delivered_customer_date < ' ~ a ~ '.order_delivered_carrier_date'
        }) }}
    {% elif table_name == 'order_items' %}
        {{ return({'negative_price': a ~ '.price < 0', 'negative_freight': a ~ '.freight_value < 0'}) }}
    {% elif table_name == 'order_payments' %}
        {{ return({'negative_payment': a ~ '.payment_value < 0', 'invalid_installments': a ~ '.payment_installments < 0',
            'unknown_payment_type': a ~ ".payment_type IS NOT NULL AND LOWER(TRIM(" ~ a ~ ".payment_type)) NOT IN ('credit_card','boleto','voucher','debit_card','not_defined')"}) }}
    {% elif table_name == 'order_reviews' %}
        {{ return({'invalid_review_score': a ~ '.review_score NOT BETWEEN 1 AND 5', 'answer_before_creation': a ~ '.review_answer_timestamp < ' ~ a ~ '.review_creation_date'}) }}
    {% elif table_name == 'products' %}
        {{ return({
            'negative_product_weight': a ~ '.product_weight_g < 0',
            'negative_product_length': a ~ '.product_length_cm < 0',
            'negative_product_height': a ~ '.product_height_cm < 0',
            'negative_product_width': a ~ '.product_width_cm < 0'
        }) }}
    {% elif table_name == 'sellers' %}
        {{ return({'invalid_seller_state': a ~ '.seller_state IS NOT NULL AND UPPER(TRIM(' ~ a ~ '.seller_state)) NOT IN ' ~ states}) }}
    {% elif table_name == 'geolocation' %}
        {{ return({
            'invalid_geolocation_state': a ~ '.geolocation_state IS NOT NULL AND UPPER(TRIM(' ~ a ~ '.geolocation_state)) NOT IN ' ~ states,
            'invalid_latitude': a ~ '.geolocation_lat NOT BETWEEN -90 AND 90',
            'invalid_longitude': a ~ '.geolocation_lng NOT BETWEEN -180 AND 180'
        }) }}
    {% elif table_name == 'product_category_translation' %}
        {{ return({'missing_category_translation': 'NULLIF(TRIM(' ~ a ~ '.product_category_name_english), \'\') IS NULL'}) }}
    {% else %}
        {{ exceptions.raise_compiler_error('No staging quality rules for ' ~ table_name) }}
    {% endif %}
{% endmacro %}

{% macro staging_is_valid(table_name, alias) %}
    {% set rules = staging_quality_rules(table_name, alias) %}
    NOT (
      {% for reason, predicate in rules.items() %}
        COALESCE(({{ predicate }}), FALSE){{ ' OR ' if not loop.last }}
      {% endfor %}
    )
{% endmacro %}
