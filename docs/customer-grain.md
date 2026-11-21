# Customer grain and SCD2 contract

- `stg_customers` has one current certified record per source `customer_id`. Orders use this key to find their associated customer record and keep their own delivery geography.
- `customer_unique_id` identifies the person across orders. `dim_customer` rebuilds the person's geography history from dated, valid orders joined to valid customer records with a usable person key.
- A version begins at the first purchase in a run of identical ZIP prefix, city, and state values. The lower bound is inclusive and the next version's start is the exclusive upper bound. A null upper bound marks the latest observed version. When a person has simultaneous purchases, the lowest `order_id` determines the person-level observation. The version key is derived from the person key and the version start.
- Order, item, payment, and review facts resolve the version at the parent order's purchase time. If the person, purchase time, or applicable version is unavailable, the fact uses the unknown customer member.

## Corrections and replay

The intervals represent geography observed on purchases. Source `updated_at` tracks when a row changed in the database. A correction to the same source record restates its purchase observation. Invalidated or deleted observations leave the rebuilt timeline. Late purchases can split an existing interval, and incremental fact fingerprints include the resolved customer key so existing facts can be re-keyed.

See [ADR-0009](adr/0009-rebuildable-customer-history.md).

## Verification

1. Run `transform_marts` after staging succeeds. Its build and validation tasks cover the mart and API contract views.
2. Check `assert_dim_customer_nonoverlapping_versions` and `assert_fact_customer_as_of_purchase` in the marts validation output.
3. Run `tests/integration/test_customer_history_rebuild.py` through the disposable integration stack to verify late-purchase re-keying and parity with a full rebuild from certified raw input.
