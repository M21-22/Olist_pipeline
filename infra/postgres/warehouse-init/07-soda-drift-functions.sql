-- Soda drift defaults and clean-capture profiling.
CREATE OR REPLACE FUNCTION audit.soda_default_drift_floor(metric TEXT, center DOUBLE PRECISION)
RETURNS DOUBLE PRECISION
LANGUAGE sql IMMUTABLE STRICT
AS $$
    SELECT CASE
        WHEN metric IN ('mart_rows', 'batch_rows') THEN GREATEST(ABS(center) * 0.5, 1)
        WHEN metric = 'mean_payment' THEN GREATEST(ABS(center) * 0.2, 0.01)
        ELSE 0.02
    END;
$$;

-- Walk forward through the capture using only baseline observations known at each window.
CREATE OR REPLACE FUNCTION audit.profile_soda_drift(clean_from TIMESTAMPTZ, clean_until TIMESTAMPTZ)
RETURNS TABLE (
    metric_name TEXT, object_name TEXT,
    first_source_interval_end TIMESTAMPTZ, last_source_interval_end TIMESTAMPTZ,
    fit_samples INTEGER, absolute_floor DOUBLE PRECISION, validation_samples INTEGER,
    default_validation_warnings INTEGER, calibrated_validation_warnings INTEGER
)
LANGUAGE sql STABLE
AS $$
    WITH observations AS (
        SELECT DISTINCT ON (metric_name, object_name, source_interval_end)
               metric_name, object_name, source_interval_end, metric_value, baseline_eligible
        FROM audit.metrics_history
        WHERE measured_at >= clean_from
          AND measured_at < clean_until
          AND source_interval_end IS NOT NULL
          AND metric_name <> 'checkpoint_age_minutes'
        ORDER BY metric_name, object_name, source_interval_end, measured_at
    ),
    scored AS (
        SELECT current_sample.*, ABS(current_sample.metric_value - reference.mean) AS deviation,
               3 * reference.spread AS spread_limit,
               audit.soda_default_drift_floor(current_sample.metric_name, reference.mean) AS default_floor,
               ROW_NUMBER() OVER (
                   PARTITION BY current_sample.metric_name, current_sample.object_name
                   ORDER BY current_sample.source_interval_end DESC
               ) <= 7 AS validation
        FROM observations AS current_sample
        LEFT JOIN LATERAL (
            SELECT AVG(metric_value) AS mean, STDDEV_SAMP(metric_value) AS spread
            FROM (
                SELECT previous.metric_value
                FROM observations AS previous
                WHERE previous.metric_name = current_sample.metric_name
                  AND previous.object_name = current_sample.object_name
                  AND previous.source_interval_end < current_sample.source_interval_end
                  AND previous.baseline_eligible
                ORDER BY previous.source_interval_end DESC
                LIMIT 30
            ) AS recent
            HAVING COUNT(*) >= 7
        ) AS reference ON TRUE
    ),
    fitted AS (
        SELECT metric_name, object_name,
               MIN(source_interval_end) AS first_source_interval_end,
               MAX(source_interval_end) AS last_source_interval_end,
               COUNT(deviation) FILTER (WHERE NOT validation)::integer AS fit_samples,
               GREATEST(
                   PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY deviation)
                       FILTER (WHERE NOT validation),
                   CASE WHEN metric_name IN ('mart_rows', 'batch_rows') THEN 1
                        WHEN metric_name = 'mean_payment' THEN 0.01 ELSE 0.001 END
               ) AS absolute_floor
        FROM scored
        GROUP BY metric_name, object_name
    )
    SELECT fitted.*,
           COUNT(scored.deviation)::integer AS validation_samples,
           COUNT(*) FILTER (
               WHERE deviation > GREATEST(spread_limit, default_floor)
           )::integer AS default_validation_warnings,
           COUNT(*) FILTER (
               WHERE deviation > GREATEST(spread_limit, fitted.absolute_floor)
           )::integer AS calibrated_validation_warnings
    FROM fitted
    JOIN scored USING (metric_name, object_name)
    WHERE scored.validation
    GROUP BY fitted.metric_name, fitted.object_name, first_source_interval_end,
             last_source_interval_end, fit_samples, absolute_floor;
$$;
