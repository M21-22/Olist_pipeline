-- Reject potentially identifying/free-text fields from the public API schema.
-- This is a defense-in-depth check; endpoint response allowlists remain essential.
SELECT table_name, column_name
FROM information_schema.columns
WHERE table_schema = 'api'
  AND table_name IN (
      'orders', 'order_items', 'seller_delivery_performance',
      'sla_alerts', 'daily_kpis', 'categories_summary'
  )
  AND (
      column_name ~* '(email|phone|address|first_name|last_name|comment|message|password|token|secret)'
      OR column_name IN ('customer_id', 'customer_unique_id', 'customer_sk', 'customer_zip_code_prefix')
  )
