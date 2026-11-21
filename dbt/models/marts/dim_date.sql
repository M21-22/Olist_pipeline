-- Static date spine for original Olist history and extended demo/forecast dates.
-- Fixed bounds keep surrogate keys and row coverage stable across rebuilds.
WITH calendar AS (
    SELECT series_day::date AS calendar_date
    FROM GENERATE_SERIES(
        '2016-01-01'::timestamp,
        '2035-12-31'::timestamp,
        INTERVAL '1 day'
    ) AS series_day
),
known_dates AS (
    SELECT
        TO_CHAR(calendar_date, 'YYYYMMDD')::integer AS date_sk,
        calendar_date::date AS calendar_date,
        EXTRACT(YEAR FROM calendar_date)::integer AS year,
        EXTRACT(QUARTER FROM calendar_date)::integer AS quarter,
        EXTRACT(MONTH FROM calendar_date)::integer AS month,
        EXTRACT(DAY FROM calendar_date)::integer AS day_of_month,
        EXTRACT(ISODOW FROM calendar_date)::integer AS iso_day_of_week,
        EXTRACT(WEEK FROM calendar_date)::integer AS iso_week_of_year,
        DATE_TRUNC('month', calendar_date)::date AS month_start_date,
        (EXTRACT(ISODOW FROM calendar_date) IN (6, 7)) AS is_weekend,
        FALSE AS is_inferred
    FROM calendar
),
unknown_member AS (
    SELECT
        0::integer AS date_sk,
        NULL::date AS calendar_date,
        NULL::integer AS year,
        NULL::integer AS quarter,
        NULL::integer AS month,
        NULL::integer AS day_of_month,
        NULL::integer AS iso_day_of_week,
        NULL::integer AS iso_week_of_year,
        NULL::date AS month_start_date,
        NULL::boolean AS is_weekend,
        TRUE AS is_inferred
)
SELECT * FROM known_dates
UNION ALL
SELECT * FROM unknown_member
