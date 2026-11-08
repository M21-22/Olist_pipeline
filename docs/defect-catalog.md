
# Defect Catalog

This catalog lists known failure modes in the Olist pipeline, the gate responsible for detecting each defect, and the expected action.

Detection rate is the percentage of injected defects correctly identified by the intended gate and handled according to the expected action.

## Action Codes

- **Q-B**: Quarantine entire batch
- **R-R**: Reject rows to `quarantine.rejected_rows`
- **BLK**: Block publish/serving
- **WARN**: Alert only (data still loads)
- **FIX**: Auto-repair with alert

## Defect Matrix

| ID | Defect | Injected by | Detected by (gate) | Action |
|---|---|---|---|---|
| D01 | Duplicate rows within a batch | Emulator duplicate-key bug | GE key uniqueness (G1) + dbt `unique` (G3) | R-R / dedupe |
| D02 | Duplicates across batches (overlap re-extract) | Watermark overlap (by design) | Staging dedupe; dbt `unique` (G3) | Dedupe |
| D03 | Corrupt or truncated Parquet | Chaos: truncate file | Readability + checksum (G1) | Q-B |
| D04 | Manifest/row-count mismatch | Chaos: edit count or drop rows | Manifest check (G1), load count (G2) | Q-B |
| D05 | Schema drift: added nullable column | Chaos: add column in source | GE schema contract (G1) | WARN, hold column |
| D06 | Schema drift: renamed/removed/type-changed column | Chaos: DDL on source | GE schema contract (G1) | Q-B |
| D07 | Nulls in required fields | Emulator bad values | GE not-null (G1) | Q-B or R-R by field |
| D08 | Out-of-range or impossible values (negative price, score outside 1-5, future timestamps, wrong timestamp order) | Emulator bad values | GE ranges (G1); dbt singular tests (G3) | R-R |
| D09 | Invalid categoricals (unknown status, invalid state code) | Emulator bad values | GE value sets (G1); dbt `accepted_values` (G3) | R-R / Q-B for status |
| D10 | Orphan foreign keys (item before product/seller) | Emulator lazy master data | dbt `relationships` with severity (G3) | `unknown` member, WARN |
| D11 | Late or out-of-order updates | Chaos: delay batches, swap order | Staging latest-wins; end-state equals source (G4 reconciliation) | Correct by design |
| D12 | Silent updates (`updated_at` not bumped) | Emulator trigger bypass | Row-hash reconciliation | FIX + alert |
| D13 | Hard deletes in source | Emulator deletes | Key reconciliation | FIX (tombstone) + alert |
| D14 | Source vs warehouse count drift | Chaos: drop rows in raw | Reconciliation + Soda on result table | BLK + FIX |
| D15 | Missing or stale batch (extract did not run) | Chaos: pause DAG | Soda freshness; watermark-lag alert; zero-row heartbeat | WARN, BLK if past SLA |
| D16 | Volume anomaly (batch far smaller or larger than baseline) | Chaos: scale a batch | Soda volume check vs baseline | WARN or BLK by size |
| D17 | Distribution drift (null rate, mean payment, `unknown`-member share) | Chaos: skew a column | Soda distribution checks | WARN |
| D18 | SCD2 overlap or multiple current rows | Chaos: corrupt snapshot | dbt singular tests (G3) | BLK |
| D19 | Text noise (whitespace, case variants) | Emulator noise | Normalization in staging; dbt `accepted_values` post-normalization | Normalize |
| D20 | PII or non-allowlisted field in an API response | Chaos: add column to `api` view | dbt contract + Schemathesis + response-model test | CI fail |
| D21 | Aggregate inconsistency (items + freight vs payments) | Chaos: alter payments | dbt singular test with calibrated tolerance; Soda cross-table | WARN or BLK |
| D22 | Replay of an already-loaded batch | Chaos: rerun DAG run | Unique `batch_registry` key; idempotent load | No-op, logged |
| D23 | Watermark advances despite failed batch | Chaos: fail load after extraction | Batch registry and watermark consistency (G2) | BLK + recovery |
| D24 | Partial warehouse load after interruption | Chaos: terminate loader mid-batch | Transaction rollback and batch-count reconciliation (G2) | Retry idempotently |
| D25 | Duplicate or concurrent extraction of the same interval | Chaos: overlapping DAG runs | Batch registry and interval ownership (G2) | Skip or serialize |

## Validation Strategy

Each defect must have:
- A reproducible injection mechanism
- An expected detection gate
- An expected action
- An automated test implemented in the relevant phase

Detection rate is calculated as:

detected injected defects / total injected defects * 100

A defect counts as detected only when the intended gate identifies it and the expected action is executed.

Test results will be recorded during Phase 8.

New defects found by the fuzz job in Phase 8 will be appended with their ID, detection check, and automated test.

## Implementation Status

All defects are currently planned, not implemented.

The catalog defines expected behavior; it does not claim that any validation mechanism is operational.

Ambiguous actions and thresholds will be finalized during the implementation of the corresponding validation gates.
