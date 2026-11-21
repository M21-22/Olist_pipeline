-- API consumers must see contract views but not internal transformation tables.
-- PostgreSQL privilege functions inspect effective grants, including role inheritance.
WITH expected_api_views AS (
    SELECT unnest(ARRAY[
        'orders',
        'order_items',
        'seller_delivery_performance',
        'sla_alerts',
        'daily_kpis',
        'categories_summary'
    ]) AS relation_name
),
violations AS (
    SELECT 'api schema USAGE missing' AS issue
    WHERE NOT has_schema_privilege('api_reader', 'api', 'USAGE')

    UNION ALL

    SELECT 'api_reader lacks SELECT on api.' || relation_name
    FROM expected_api_views
    WHERE NOT has_table_privilege(
        'api_reader', 'api.' || relation_name, 'SELECT'
    )

    UNION ALL

    SELECT 'api_reader can SELECT marts.fct_orders'
    WHERE has_table_privilege('api_reader', 'marts.fct_orders', 'SELECT')

    UNION ALL

    SELECT 'api_reader can SELECT staging.stg_orders'
    WHERE has_table_privilege('api_reader', 'staging.stg_orders', 'SELECT')

    UNION ALL

    SELECT 'api_reader can SELECT raw.orders'
    WHERE has_table_privilege('api_reader', 'raw.orders', 'SELECT')
)
SELECT issue
FROM violations
