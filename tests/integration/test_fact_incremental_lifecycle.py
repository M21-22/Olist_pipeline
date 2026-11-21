"""Exercise fact merges against the disposable warehouse_test database only."""

import os
import subprocess
from pathlib import Path
from uuid import uuid4

import psycopg
import pytest

pytestmark = pytest.mark.integration

if os.environ.get('OLIST_RUN_DB_INTEGRATION') != '1' or os.environ.get('OLIST_TEST_DBT') != '1':
    pytest.skip('Disposable dbt integration is disabled', allow_module_level=True)

DBT = '/home/airflow/dbt-venv/bin/dbt'
FACTS = ('fct_orders', 'fct_order_items', 'fct_payments', 'fct_reviews')
STAGING = (
    'stg_orders', 'stg_order_items', 'stg_order_payments', 'stg_order_reviews',
    'stg_customers', 'stg_products', 'stg_sellers',
    'stg_geolocation', 'stg_product_category_translation',
)
DIMENSIONS = ('dim_customer', 'dim_product', 'dim_seller', 'dim_geography', 'dim_date')


def test_fact_merge_update_invalidation_late_dimension_and_repeatability(tmp_path: Path):
    """New rows, remerges, changed attributes and invalidations stay correct."""
    profile = tmp_path / 'profiles.yml'
    profile.write_text(
        "olist:\n  target: dev\n  outputs:\n    dev:\n"
        "      type: postgres\n      host: test-db\n      port: 5432\n"
        "      dbname: warehouse_test\n      user: dbt_runner\n"
        "      password: \"{{ env_var('DBT_RUNNER_PASSWORD') }}\"\n"
        "      schema: staging\n      threads: 1\n",
        encoding='utf-8',
    )

    def dbt(*args):
        result = subprocess.run(
            [DBT, *args, '--project-dir', '/opt/airflow/dbt',
             '--profiles-dir', str(tmp_path),
             '--target-path', str(tmp_path / 'target'),
             '--log-path', str(tmp_path / 'logs')],
            capture_output=True, text=True, check=False, timeout=300,
            env=os.environ.copy(),
        )
        assert result.returncode == 0, (
            f"dbt {' '.join(args)} failed:\n"
            f"{result.stdout[-9000:]}\n{result.stderr[-3000:]}"
        )

    def connect():
        return psycopg.connect(
            host='test-db', dbname='warehouse_test', user='dbt_runner',
            password=os.environ['DBT_RUNNER_PASSWORD'],
        )

    def query(sql, params=()):
        with connect() as conn, conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall()

    def execute(sql, params=()):
        with connect() as conn, conn.cursor() as cur:
            cur.execute(sql, params)

    with connect() as conn, conn.cursor() as cur:
        cur.execute('SELECT current_database()')
        assert cur.fetchone()[0] == 'warehouse_test', 'Refusing non-test database'

    # Bootstrap empty staging and dimension relations from the real dbt code.
    # This bypasses ingestion on purpose: this test targets only fact behavior.
    dbt('run', '--select', *STAGING)
    dbt('run', '--select', *DIMENSIONS)

    token = uuid4().hex
    order_id = 'fact-' + token
    customer_id = 'cust-' + token
    product_id = 'product-' + token
    seller_id = 'seller-' + token
    item_pk = int(token[:12], 16)
    review_pk = int(token[12:24], 16)
    batch_id = 'test-fact-' + token

    execute(
        """INSERT INTO staging.stg_orders
           (order_id, customer_id, order_purchase_timestamp,
            order_estimated_delivery_date, updated_at, order_status,
            source_batch_id, is_valid)
           VALUES (%s, %s, '2017-03-01 10:00+00', '2017-03-10 10:00+00',
                   '2017-03-01 11:00+00', 'approved', %s, TRUE)""",
        (order_id, customer_id, batch_id),
    )
    execute(
        """INSERT INTO staging.stg_order_items
           (item_pk, order_id, order_item_id, product_id, seller_id,
            shipping_limit_date, price, freight_value, updated_at,
            source_batch_id, is_valid)
           VALUES (%s, %s, 1, %s, %s, '2017-03-06 10:00+00', 20, 5,
                   '2017-03-01 11:00+00', %s, TRUE)""",
        (item_pk, order_id, product_id, seller_id, batch_id),
    )
    execute(
        """INSERT INTO staging.stg_order_payments
           (order_id, payment_sequential, payment_type, payment_installments,
            payment_value, updated_at, source_batch_id, is_valid)
           VALUES (%s, 1, 'credit_card', 1, 25,
                   '2017-03-01 11:00+00', %s, TRUE)""",
        (order_id, batch_id),
    )
    execute(
        """INSERT INTO staging.stg_order_reviews
           (review_pk, review_id, order_id, review_score, review_creation_date,
            updated_at, source_batch_id, is_valid)
           VALUES (%s, %s, %s, 4, '2017-03-11 10:00+00',
                   '2017-03-11 11:00+00', %s, TRUE)""",
        (review_pk, 'review-' + token, order_id, batch_id),
    )

    def build_facts():
        dbt('run', '--select', *FACTS)

    def rows():
        return {
            fact: query(
                'SELECT COUNT(*) FROM marts.' + fact + ' WHERE order_id = %s',
                (order_id,),
            )[0][0]
            for fact in FACTS
        }

    def fingerprint(table, key_col, key):
        return query(
            'SELECT row_fingerprint FROM marts.' + table +
            ' WHERE ' + key_col + ' = %s', (key,),
        )[0][0]

    build_facts()
    initial = rows()
    assert initial == {name: 1 for name in FACTS}, initial
    first_fp = fingerprint('fct_orders', 'order_id', order_id)
    unknown_product = query(
        'SELECT product_sk FROM marts.fct_order_items WHERE item_pk = %s',
        (item_pk,),
    )[0][0]
    assert unknown_product == query("SELECT md5('__olist_unknown_product__')")[0][0]

    # No source changes: every fact remains stable across another run.
    build_facts()
    assert rows() == initial
    assert fingerprint('fct_orders', 'order_id', order_id) == first_fp

    # A payment-only change must refresh the order's aggregate, too.
    execute(
        'UPDATE staging.stg_order_payments SET payment_value = 30 '
        'WHERE order_id = %s AND payment_sequential = 1', (order_id,),
    )
    build_facts()
    assert query('SELECT payment_total FROM marts.fct_orders WHERE order_id = %s',
                 (order_id,))[0][0] == 30
    assert query('SELECT payment_value FROM marts.fct_payments WHERE order_id = %s',
                 (order_id,))[0][0] == 30
    assert fingerprint('fct_orders', 'order_id', order_id) != first_fp

    # A product arrives after the item: unknown FK becomes the real product SK.
    execute(
        """INSERT INTO staging.stg_products
           (product_id, product_category_name, updated_at, source_batch_id, is_valid)
           VALUES (%s, 'books_general_interest', '2017-03-12 10:00+00', %s, TRUE)""",
        (product_id, batch_id),
    )
    dbt('run', '--select', 'dim_product')
    build_facts()
    actual_product = query(
        'SELECT product_sk FROM marts.fct_order_items WHERE item_pk = %s',
        (item_pk,),
    )[0][0]
    assert actual_product == query('SELECT product_sk FROM marts.dim_product WHERE product_id = %s',
                                   (product_id,))[0][0]
    assert actual_product != unknown_product

    # An invalid current winner is removed on the next incremental run.
    execute('UPDATE staging.stg_order_reviews SET is_valid = FALSE WHERE review_pk = %s',
            (review_pk,))
    execute('UPDATE staging.stg_order_items SET is_valid = FALSE WHERE item_pk = %s',
            (item_pk,))
    build_facts()
    assert query('SELECT COUNT(*) FROM marts.fct_reviews WHERE review_pk = %s',
                 (review_pk,))[0][0] == 0
    assert query('SELECT COUNT(*) FROM marts.fct_order_items WHERE item_pk = %s',
                 (item_pk,))[0][0] == 0
    assert query('SELECT item_count FROM marts.fct_orders WHERE order_id = %s',
                 (order_id,))[0][0] == 0

    # Tombstone-like disappearance from staging removes the order and payment.
    execute('DELETE FROM staging.stg_orders WHERE order_id = %s', (order_id,))
    execute('DELETE FROM staging.stg_order_payments WHERE order_id = %s', (order_id,))
    build_facts()
    assert rows() == {name: 0 for name in FACTS}
