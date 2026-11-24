-- Invoke through make with an explicitly selected defect-free capture period.
-- Fail before writing when the requested time bounds are reversed or empty.
SELECT 1 / ((:'clean_from'::timestamptz < :'clean_until'::timestamptz)::integer)
    AS valid_capture_range \gset

CREATE TEMP TABLE drift_profile ON COMMIT DROP AS
SELECT * FROM audit.profile_soda_drift(:'clean_from'::timestamptz, :'clean_until'::timestamptz);

SELECT metric_name, object_name, fit_samples, validation_samples,
       CASE WHEN fit_samples >= 14 AND validation_samples >= 7
            THEN ROUND(absolute_floor::numeric, 6) END AS proposed_floor,
       default_validation_warnings, calibrated_validation_warnings,
       CASE WHEN fit_samples < 14 OR validation_samples < 7 THEN 'INSUFFICIENT_HISTORY'
            WHEN calibrated_validation_warnings > default_validation_warnings THEN 'MORE_WARNINGS'
            ELSE 'READY' END AS decision
FROM drift_profile
ORDER BY metric_name, object_name;

\if :apply
INSERT INTO audit.soda_drift_calibration (
    metric_name, object_name, absolute_floor, fit_samples, validation_samples,
    default_validation_warnings, calibrated_validation_warnings,
    first_source_interval_end, last_source_interval_end
)
SELECT metric_name, object_name, absolute_floor, fit_samples, validation_samples,
       default_validation_warnings, calibrated_validation_warnings,
       first_source_interval_end, last_source_interval_end
FROM drift_profile
WHERE fit_samples >= 14 AND validation_samples >= 7
  AND calibrated_validation_warnings <= default_validation_warnings
ON CONFLICT (metric_name, object_name) DO UPDATE SET
    absolute_floor = EXCLUDED.absolute_floor,
    fit_samples = EXCLUDED.fit_samples,
    validation_samples = EXCLUDED.validation_samples,
    default_validation_warnings = EXCLUDED.default_validation_warnings,
    calibrated_validation_warnings = EXCLUDED.calibrated_validation_warnings,
    first_source_interval_end = EXCLUDED.first_source_interval_end,
    last_source_interval_end = EXCLUDED.last_source_interval_end,
    calibrated_at = clock_timestamp();
\endif
