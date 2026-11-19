"""Static checks for staging schema and Gate 3 data tests."""
from pathlib import Path
import yaml

ROOT = Path(__file__).resolve().parents[2]
EXPECTED = {
    'stg_customers': ('customer_id',),
    'stg_orders': ('order_id',),
    'stg_order_items': ('item_pk',),
    'stg_order_payments': ('order_id', 'payment_sequential'),
    'stg_order_reviews': ('review_pk',),
    'stg_products': ('product_id',),
    'stg_sellers': ('seller_id',),
    'stg_geolocation': ('geolocation_pk',),
    'stg_product_category_translation': ('product_category_name',),
}

def test_schema_covers_all_staging_models_and_required_keys():
    doc = yaml.safe_load((ROOT / 'dbt/models/staging/schema.yml').read_text())
    models = {x['name']: x for x in doc['models']}
    assert models.keys() == EXPECTED.keys()
    for name, keys in EXPECTED.items():
        cols = {x['name']: x['data_tests'] for x in models[name]['columns']}
        for key in keys:
            assert 'not_null' in cols[key], (name, key)
            if len(keys) == 1:
                assert 'unique' in cols[key], (name, key)
        for field in ('is_valid','source_batch_id'):
            assert 'not_null' in cols[field], (name, field)

def test_valid_value_filters_match_rejection_policy():
    models = yaml.safe_load((ROOT / 'dbt/models/staging/schema.yml').read_text())['models']
    for name, field in [('stg_orders','order_status'),('stg_order_payments','payment_type')]:
        columns = {x['name']: x for x in next(x for x in models if x['name']==name)['columns']}
        accepted = next(x['accepted_values'] for x in columns[field]['data_tests'] if isinstance(x,dict) and 'accepted_values' in x)
        assert accepted['config']['where'] == 'is_valid = TRUE'
    macro=(ROOT/'dbt/macros/staging_quality.sql').read_text()
    assert 'unknown_payment_type' in macro

def test_composite_payment_grain_test_exists():
    sql=(ROOT/'dbt/tests/assert_staging_payment_unique.sql').read_text()
    assert 'GROUP BY order_id, payment_sequential' in sql
    assert 'HAVING COUNT(*) > 1' in sql
