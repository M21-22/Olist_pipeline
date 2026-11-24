# ADR-0008: Staging Incremental Processing and Reject Routing

## Status
Accepted

## Context

Batch certification can arrive late with a timestamp older than one already seen by staging. A fixed lookback or a certification high-water mark alone can miss that batch. Separately, a newer version of a business record can fail a value rule. Filtering invalid rows before ranking can cause an older valid version to appear current.

## Decision

- For the seven mutable sources, use PostgreSQL dbt incremental `merge` with each model's business key. At the start of the model transaction, acquire a per-model advisory lock and capture certified batch IDs through the run cutoff that are absent from `audit.dbt_staging_processed_batches`. Use that batch set to identify affected keys, then rank their certified history by source update timestamp, certification timestamp, batch ID, and row hash. Commit the processed markers and model changes together. `audit.dbt_staging_progress` records the cutoff and progress. Eligibility on later runs is determined by the processed-batch ledger.
- Rebuild geolocation and category translation from the latest certified snapshot. Apply `audit.deleted_keys` tombstones to mutable staging models.
- Retain the current winner with its `source_batch_id` and `is_valid` flag. A newer invalid winner supersedes the older valid state. A newer valid version restores the key to valid downstream inputs.
- After Gate 3 passes, sync current invalid winners to `quarantine.rejected_rows` with raw payload provenance and one entry per reason. The insert is idempotent by source table, business key, batch ID, and reason code. A correction clears the current rejection candidate and leaves the historical quarantine entry available for audit.

## Alternatives Considered
- Fixed 24-hour lookback with reconciliation for older batches
- Certification timestamp as the sole incremental cursor
- Filter invalid versions before selecting the current version

## Rationale
Tracking processed batch IDs accounts for late certification independently
of arrival order. Selecting the current version before applying value rules
keeps an invalid update from making a superseded valid version look current.

## Consequences

A failed model transaction rolls back its staging changes and processed markers together. A late-certified batch is eligible whenever it is visible at capture and still absent from the ledger. The ledger grows with processed batches.

Reject sync covers current invalid winners observed after a successful Gate 3 check. An invalid version superseded before sync can be absent from quarantine. Capturing every invalid raw version would require validating certified versions before staging deduplication.

Mart business inputs must read valid staging rows. Incremental marts must explicitly handle valid-to-invalid transitions so a previously emitted row does not stay current simply because it disappeared from the filtered input. Invalid customer and order observations are excluded from the purchase-time customer history until corrected. Certified source deletions are handled through tombstones. See [ADR-0009](0009-rebuildable-customer-history.md). API views inherit this policy through marts.
