-- Alert-only view must exactly match source mart's flagged seller-day keys.
WITH expected AS (
    SELECT seller_id, seller_state, delivery_date
    FROM {{ ref('mart_seller_delivery_sla') }}
    WHERE is_sla_alert IS TRUE
),
actual AS (
    SELECT seller_id, seller_state, delivery_date
    FROM {{ ref('sla_alerts') }}
),
differences AS (
    (SELECT * FROM expected EXCEPT ALL SELECT * FROM actual)
    UNION ALL
    (SELECT * FROM actual EXCEPT ALL SELECT * FROM expected)
)
SELECT * FROM differences
