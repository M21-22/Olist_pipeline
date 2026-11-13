# Deletion key contract

`audit.deleted_keys` uses `(source_table, business_key)` as its primary key.

- Single-column source primary keys: `pk::TEXT`.
- `order_payments` composite primary key: `JSONB_BUILD_ARRAY(order_id, payment_sequential)::TEXT`.
- Both source reconciliation SQL and warehouse staging SQL must use the same PostgreSQL expression. Do not use Python string concatenation or a pipe delimiter.
- Reconciliation must insert/upsert a key when the record is absent from the source, and DELETE the tombstone when the record appears again. Otherwise a subsequently restored row remains excluded forever.
- Only confirmed deletions should be written, not temporary mismatches caused by uncertified or delayed landing.

Source-side payment-key query:

```sql
SELECT JSONB_BUILD_ARRAY(order_id, payment_sequential)::TEXT AS business_key
FROM shop.order_payments;
```

Warehouse-side payment-key query (when inspecting current, certified rows):

```sql
SELECT JSONB_BUILD_ARRAY(order_id, payment_sequential)::TEXT AS business_key
FROM staging.stg_order_payments;
```

The reconciliation procedure itself is not implemented in these files; it must incorporate the same expression in its comparison and repair/tombstone handling.
