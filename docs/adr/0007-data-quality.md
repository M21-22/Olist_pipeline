# ADR-0007: Layered Data Quality Validation

## Status
Accepted

## Context
The platform must detect corrupted data, invalid schemas,
incorrect transformations, missing records, and freshness problems.

A single validation tool cannot reliably cover all failure modes.

## Decision
Use layered data quality gates:

Gate 1:
- Custom checks validate landed manifests and Parquet integrity and schema.
- Great Expectations checks required keys and the extraction timestamp window.

Gate 2:
- Custom load checks reconcile raw rows and control totals with the manifest
  before certification.

Gate 3:
- dbt tests validate staging and its transformation invariants. The staging
  DAG syncs row-level rejections after those tests pass. See [ADR-0008](0008-staging-incremental-and-rejection-policy.md).

Gate 4:
- Phase 7 plans Soda checks for mart freshness, volume, and drift.
- Source-to-certified-raw reconciliation is implemented separately. The plan
  reserves downstream reconciliation and serving-state checks for Phase 7.

Persist ingestion quality results in `audit.dq_results` and reconciliation
outcomes in `audit.reconciliation`. Gate 3 results are visible in Airflow
task logs. Warehouse export of dbt run results is planned.

## Alternatives Considered
- Great Expectations only
- dbt tests only
- Soda only
- Fully custom Python validation framework

## Rationale
- Great Expectations supports explicit data expectations.
- dbt tests validate transformation and relational invariants.
- Soda supports freshness, volume, and drift monitoring.
- Custom reconciliation detects discrepancies between systems.
- Independent gates reduce reliance on one validation mechanism.

## Consequences
Positive:
- Clear validation responsibilities
- Better defect coverage
- Auditable validation results
- Automated detection testing

Negative:
- Multiple tools increase maintenance overhead.
- Some checks may overlap.
- Validation adds processing latency.
- Unknown corruption cannot be guaranteed to be detected.

## Implementation Constraints
- Maintain a defect catalog with expected detection mechanisms.
- Quarantine invalid batches or rows according to policy.
- Do not advance watermarks for failed batches.
- Make validation and replay idempotent.
- Verify tool versions, licenses, and capabilities before implementation.
