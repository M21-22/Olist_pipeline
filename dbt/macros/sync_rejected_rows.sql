{% macro sync_rejected_rows() %}
    {% set sql %}
        INSERT INTO quarantine.rejected_rows (
            source_table,
            business_key,
            batch_id,
            reason_code,
            raw_row
        )
        SELECT
            source_table,
            business_key,
            batch_id,
            reason_code,
            raw_row
        FROM {{ ref('rejected_rows_candidates') }}
        WHERE TRUE
        ON CONFLICT (source_table, business_key, batch_id, reason_code)
        DO NOTHING
    {% endset %}
    {% if execute %}
        {% do run_query(sql) %}
    {% endif %}
{% endmacro %}
