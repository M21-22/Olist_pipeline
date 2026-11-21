-- Olist geolocation has multiple samples per ZIP prefix, sometimes with
-- conflicting city/state values. Use a deterministic modal location, then
-- average coordinates only from samples matching that selected location.
WITH valid_samples AS (
    SELECT
        geolocation_zip_code_prefix::text AS zip_code_prefix,
        geolocation_city::text AS city,
        geolocation_state::text AS state,
        geolocation_lat::double precision AS latitude,
        geolocation_lng::double precision AS longitude
    FROM {{ ref('stg_geolocation') }}
    WHERE
        is_valid IS TRUE
        AND geolocation_zip_code_prefix IS NOT NULL
),
location_counts AS (
    SELECT
        zip_code_prefix,
        city,
        state,
        COUNT(*)::bigint AS location_samples
    FROM valid_samples
    GROUP BY zip_code_prefix, city, state
),
selected_locations AS (
    SELECT
        zip_code_prefix,
        city,
        state,
        ROW_NUMBER() OVER (
            PARTITION BY zip_code_prefix
            ORDER BY location_samples DESC, state ASC NULLS LAST,
                     city ASC NULLS LAST
        ) AS location_rank
    FROM location_counts
),
known_geography AS (
    SELECT
        MD5('geography:' || s.zip_code_prefix)::text AS geography_sk,
        s.zip_code_prefix::text AS zip_code_prefix,
        s.city::text AS city,
        s.state::text AS state,
        AVG(v.latitude) FILTER (
            WHERE v.latitude IS NOT NULL AND v.longitude IS NOT NULL
        )::double precision AS latitude,
        AVG(v.longitude) FILTER (
            WHERE v.latitude IS NOT NULL AND v.longitude IS NOT NULL
        )::double precision AS longitude,
        COUNT(*) FILTER (
            WHERE v.latitude IS NOT NULL AND v.longitude IS NOT NULL
        )::bigint AS coordinate_samples,
        FALSE AS is_inferred
    FROM selected_locations AS s
    INNER JOIN valid_samples AS v
        ON s.zip_code_prefix = v.zip_code_prefix
        AND s.city IS NOT DISTINCT FROM v.city
        AND s.state IS NOT DISTINCT FROM v.state
    WHERE s.location_rank = 1
    GROUP BY s.zip_code_prefix, s.city, s.state
),
unknown_member AS (
    SELECT
        MD5('__olist_unknown_geography__')::text AS geography_sk,
        '__unknown__'::text AS zip_code_prefix,
        'Unknown'::text AS city,
        NULL::text AS state,
        NULL::double precision AS latitude,
        NULL::double precision AS longitude,
        0::bigint AS coordinate_samples,
        TRUE AS is_inferred
)
SELECT * FROM known_geography
UNION ALL
SELECT * FROM unknown_member
