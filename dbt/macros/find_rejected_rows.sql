{# Emit only violations belonging to the CURRENT certified staged version.
   source_batch_id resolves the exact raw payload without inventing provenance. #}
{% macro find_rejected_rows(table_name, business_key, key_match) %}
    {% set rules = staging_quality_rules(table_name, 'stg') %}
    SELECT
        '{{ table_name }}'::text AS source_table,
        ({{ business_key }})::text AS business_key,
        stg.source_batch_id::text AS batch_id,
        checks.reason_code::text AS reason_code,
        TO_JSONB(raw_src) AS raw_row
    FROM {{ ref('stg_' ~ table_name) }} AS stg
    JOIN {{ source('raw', table_name) }} AS raw_src
        ON raw_src._batch_id = stg.source_batch_id
       AND {{ key_match }}
    CROSS JOIN LATERAL (
        VALUES
        {% for reason, predicate in rules.items() %}
            ('{{ reason }}', COALESCE(({{ predicate }}), FALSE)){{ ',' if not loop.last }}
        {% endfor %}
    ) AS checks (reason_code, is_invalid)
    WHERE stg.is_valid = FALSE AND checks.is_invalid
{% endmacro %}
