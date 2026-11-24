# ADR-0009: Rebuildable customer history from purchases

## Status

Accepted.

## Context

The Olist customer record referenced by an order supplies delivery geography.
Several such records can belong to the same person through `customer_unique_id`.
The dbt snapshot captured a periodically observed representative record. That
history depended on when the snapshot ran, which the landing replay does not preserve.

## Decision

Build `dim_customer` from current valid orders and their current valid
order-associated customer records, reconstructed from certified source data.
Use `customer_unique_id` as the person key. Each dated purchase supplies one
observation of ZIP prefix, city, and state. For simultaneous purchases by the
same person, choose the smallest order ID for the person-level timeline.
Collapse consecutive identical observations. Use half-open validity ranges
beginning at the first purchase in each run of identical observations, and
derive the surrogate key from the person key and the UTC epoch of that start.

Join order, item, payment, and review facts by the parent order purchase time.
Use the existing unknown member when the source identity, purchase time, or
matching history is unavailable. Preserve order-specific delivery geography in
`fct_orders`, including when simultaneous purchases disagree.

## Consequences

The dimension represents geography observed on purchases. Olist supplies no
move date. Corrections to the same order-specific record restate the observed
purchase, and invalidated or deleted observations leave the rebuilt timeline.
Late purchases can split an existing range. The incremental fact fingerprints
include the resolved customer key so previously loaded facts can be updated.

The acceptance checks cover non-overlapping ranges, purchase-time fact keys,
and parity between incremental runs and a full dbt rebuild from certified raw
data. The landing replay drill also verifies the upstream reconstruction.
