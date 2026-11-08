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
- Great Expectations validates landed files, schemas, and values.

Gate 2:
- Custom reconciliation validates raw loads against manifests.

Gate 3:
- dbt tests and Great Expectations validate staging data.

Gate 4:
- dbt tests, Soda, and source-to-warehouse reconciliation
  validate analytical outputs.

Record validation results in the audit schema.

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
