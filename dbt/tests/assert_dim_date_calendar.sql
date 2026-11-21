-- Validate date bounds, no gaps, date-key representation and calendar fields.
WITH expected AS (
    SELECT series_day::date AS calendar_date
    FROM GENERATE_SERIES('2016-01-01'::timestamp, '2035-12-31'::timestamp,
                         INTERVAL '1 day') AS series_day
),
missing AS (
    SELECT calendar_date FROM expected
    EXCEPT
    SELECT calendar_date FROM {{ ref('dim_date') }} WHERE is_inferred IS FALSE
),
unexpected AS (
    SELECT calendar_date FROM {{ ref('dim_date') }} WHERE is_inferred IS FALSE
    EXCEPT
    SELECT calendar_date FROM expected
),
invalid AS (
    SELECT calendar_date
    FROM {{ ref('dim_date') }}
    WHERE is_inferred IS FALSE AND (
        date_sk IS DISTINCT FROM TO_CHAR(calendar_date, 'YYYYMMDD')::integer
        OR year IS DISTINCT FROM EXTRACT(YEAR FROM calendar_date)::integer
        OR quarter IS DISTINCT FROM EXTRACT(QUARTER FROM calendar_date)::integer
        OR month IS DISTINCT FROM EXTRACT(MONTH FROM calendar_date)::integer
        OR day_of_month IS DISTINCT FROM EXTRACT(DAY FROM calendar_date)::integer
        OR iso_day_of_week IS DISTINCT FROM EXTRACT(ISODOW FROM calendar_date)::integer
        OR iso_week_of_year IS DISTINCT FROM EXTRACT(WEEK FROM calendar_date)::integer
        OR month_start_date IS DISTINCT FROM DATE_TRUNC('month', calendar_date)::date
        OR is_weekend IS DISTINCT FROM (EXTRACT(ISODOW FROM calendar_date) IN (6, 7))
    )
)
SELECT calendar_date FROM missing
UNION ALL SELECT calendar_date FROM unexpected
UNION ALL SELECT calendar_date FROM invalid
