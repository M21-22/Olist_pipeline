{# Remove existing fact records whose own staging winner was deleted or became invalid.
   This hook runs in the same PostgreSQL transaction as the model merge. #}
{% macro fact_prune_invalid(source_model, keys) %}
    {% if is_incremental() %}
        DELETE FROM {{ this }} AS target
        WHERE NOT EXISTS (
            SELECT 1 FROM {{ ref(source_model) }} AS src
            WHERE src.is_valid IS TRUE
            {% for key in keys %}
              AND src.{{ key }} = target.{{ key }}
            {% endfor %}
        )
    {% else %}
        SELECT 1
    {% endif %}
{% endmacro %}

{# Compute a canonical output fingerprint. Only rows whose projected values changed
   are submitted to PostgreSQL MERGE; dependencies may change without source timestamps. #}
{% macro fact_changed_rows(key_columns) %}
    {% if is_incremental() %}
        LEFT JOIN {{ this }} AS previous
          ON {% for key in key_columns %}candidate.{{ key }} = previous.{{ key }}{{ ' AND ' if not loop.last }}{% endfor %}
        WHERE previous.row_fingerprint IS DISTINCT FROM candidate.row_fingerprint
    {% endif %}
{% endmacro %}
