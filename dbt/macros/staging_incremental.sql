{#
  Track processed batch IDs rather than relying on a certification timestamp:
  late certifications can be older than the last reported cursor.
  The transaction-scoped lock serializes runs for the same staging model.
  Pending batch IDs are captured once and committed with the model update.
#}
{% macro staging_lock() %}
    SELECT pg_advisory_xact_lock(hashtextextended('olist:dbt:staging:' || '{{ this.identifier }}', 0))
{% endmacro %}

{% macro staging_prepare(source_table) %}
    INSERT INTO audit.dbt_staging_progress (
        model_name, last_certified_at, last_batch_id,
        run_cutoff_at, run_cutoff_batch_id
    )
    SELECT
        '{{ this.identifier }}', NULL::timestamptz, NULL::text,
        newest.certified_at, newest.batch_id
    FROM (SELECT 1) AS anchor
    LEFT JOIN (
        SELECT certified_at, batch_id
        FROM {{ source('audit', 'batch_registry') }}
        WHERE table_name = '{{ source_table }}'
          AND status = 'CERTIFIED'
          AND certified_at IS NOT NULL
        ORDER BY certified_at DESC, batch_id DESC
        LIMIT 1
    ) AS newest ON TRUE
    ON CONFLICT (model_name) DO UPDATE
       SET run_cutoff_at = EXCLUDED.run_cutoff_at,
           run_cutoff_batch_id = EXCLUDED.run_cutoff_batch_id,
           updated_at = CURRENT_TIMESTAMP
{% endmacro %}

{% macro staging_capture_pending(source_table) %}
    CREATE TEMP TABLE olist_staging_pending_batches ON COMMIT DROP AS
    SELECT registry.batch_id
    FROM {{ source('audit', 'batch_registry') }} AS registry
    JOIN audit.dbt_staging_progress AS progress
      ON progress.model_name = '{{ this.identifier }}'
    WHERE registry.table_name = '{{ source_table }}'
      AND registry.status = 'CERTIFIED'
      AND registry.certified_at IS NOT NULL
      AND (registry.certified_at, registry.batch_id) <=
          (COALESCE(progress.run_cutoff_at, '-infinity'::timestamptz),
           COALESCE(progress.run_cutoff_batch_id, ''))
      AND NOT EXISTS (
          SELECT 1 FROM audit.dbt_staging_processed_batches AS processed
          WHERE processed.model_name = '{{ this.identifier }}'
            AND processed.batch_id = registry.batch_id
      )
{% endmacro %}

{% macro staging_mark_processed(source_table) %}
    INSERT INTO audit.dbt_staging_processed_batches (model_name, batch_id)
    SELECT '{{ this.identifier }}', batch_id
    FROM pg_temp.olist_staging_pending_batches
    WHERE TRUE
    ON CONFLICT (model_name, batch_id) DO NOTHING
{% endmacro %}

{% macro staging_finalize() %}
    {% if var('staging_test_fail_finalize', false) and target.database == 'warehouse_test' %}
        SELECT 1 / 0
    {% else %}
    UPDATE audit.dbt_staging_progress
    SET last_certified_at = run_cutoff_at,
        last_batch_id = run_cutoff_batch_id,
        updated_at = CURRENT_TIMESTAMP
    WHERE model_name = '{{ this.identifier }}'
      AND run_cutoff_at IS NOT NULL

    {% endif %}
{% endmacro %}

{% macro staging_delete_tombstoned(source_table, key_columns) %}
    {% if is_incremental() %}
        DELETE FROM {{ this }} AS target
        WHERE EXISTS (
            SELECT 1
            FROM {{ source('audit', 'deleted_keys') }} AS deleted
            WHERE deleted.source_table = '{{ source_table }}'
              AND deleted.business_key =
                  {{ encode_business_key(key_columns | map('string') | list) }}
        )
    {% else %}
        SELECT 1
    {% endif %}
{% endmacro %}

{% macro staging_incremental_key_filter(table_name, key_columns) %}
    {% if is_incremental() %}
        INNER JOIN (
            SELECT DISTINCT
                {% for col in key_columns %}changed.{{ col }}{{ ',' if not loop.last }}{% endfor %}
            FROM {{ source('raw', table_name) }} AS changed
            INNER JOIN {{ source('audit', 'batch_registry') }} AS new_batches
                ON changed._batch_id = new_batches.batch_id
               AND new_batches.table_name = '{{ table_name }}'
               AND new_batches.status = 'CERTIFIED'
               AND new_batches.certified_at IS NOT NULL
            INNER JOIN pg_temp.olist_staging_pending_batches AS pending
                ON pending.batch_id = new_batches.batch_id
        ) AS changed_keys
            ON {% for col in key_columns %}src.{{ col }} = changed_keys.{{ col }}{{ ' AND ' if not loop.last }}{% endfor %}
    {% endif %}
{% endmacro %}
