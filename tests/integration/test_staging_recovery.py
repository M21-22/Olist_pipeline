"""Verify staging recovery and concurrency in disposable warehouse_test.

All fixtures are isolated by a random prefix. The test DB runs on tmpfs and is
never the persistent warehouse. Requires OLIST_RUN_DB_INTEGRATION=OLIST_TEST_DBT=1.
"""
import os
import subprocess
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

import psycopg
import pytest

if os.environ.get('OLIST_RUN_DB_INTEGRATION') != '1' or os.environ.get('OLIST_TEST_DBT') != '1':
    pytest.skip('Disposable dbt integration disabled', allow_module_level=True)

from airflow.providers.postgres.hooks.postgres import PostgresHook

MODELS = {
    'customers': ('customer_id', 'customer_city'),
    'orders': ('order_id', 'order_status'),
    'order_items': ('item_pk', 'price'),
    'order_payments': ('order_id', 'payment_value'),
    'order_reviews': ('review_pk', 'review_score'),
    'products': ('product_id', 'product_weight_g'),
    'sellers': ('seller_id', 'seller_city'),
}
DBT = '/home/airflow/dbt-venv/bin/dbt'


@pytest.mark.integration
def test_seven_models_late_tie_failure_concurrency_full_refresh(tmp_path: Path):
    admin = PostgresHook(postgres_conn_id='olist_test_admin')
    with admin.get_conn() as conn, conn.cursor() as cur:
        cur.execute('SELECT current_database()')
        assert cur.fetchone()[0] == 'warehouse_test', 'Refusing non-test database'
        cur.execute("SELECT to_regclass('audit.dbt_staging_processed_batches')")
        assert cur.fetchone()[0] is not None, 'Apply migration 08 in disposable DB'

    profile = tmp_path / 'profiles.yml'
    profile.write_text(
        'olist:\n  target: dev\n  outputs:\n    dev:\n      type: postgres\n'
        '      host: test-db\n      port: 5432\n      dbname: warehouse_test\n'
        '      user: dbt_runner\n'
        '      password: "{{ env_var(\'DBT_RUNNER_PASSWORD\') }}"\n'
        '      schema: staging\n      threads: 1\n'
    )

    def invoke(*dbt_args, expect_success=True, suffix='default'):
        output = tmp_path / suffix
        output.mkdir(exist_ok=True)
        p = subprocess.run(
            [DBT, *dbt_args,
             '--project-dir', '/opt/airflow/dbt', '--profiles-dir', str(tmp_path),
             '--target-path', str(output / 'target'), '--log-path', str(output / 'logs')],
            capture_output=True, text=True, timeout=240, check=False,
            env=os.environ.copy(),
        )
        if expect_success:
            assert p.returncode == 0, p.stdout[-5500:] + '\n' + p.stderr[-2500:]
        else:
            assert p.returncode != 0, 'Expected deliberately injected post-hook failure'
        return p

    prefix = 'p55-' + uuid4().hex[:14]
    # Unique logical intervals avoid colliding with other disposable fixtures.
    base = datetime(2020, 3, 1, tzinfo=timezone.utc) + timedelta(
        seconds=int(uuid4().hex[:8], 16)
    )
    # BIGINT values are well below 2^63, unique inside the disposable DB.
    numeric = int(uuid4().hex[:12], 16)
    keys = {
        'customers': (prefix + '-customer',),
        'orders': (prefix + '-order',),
        'order_items': (numeric,),
        'order_payments': (prefix + '-order', 1),
        'order_reviews': (numeric + 1,),
        'products': (prefix + '-product',),
        'sellers': (prefix + '-seller',),
    }
    values = {
        'customers': ('alpha', 'beta', 'gamma'),
        'orders': ('created', 'approved', 'delivered'),
        'order_items': (10, 20, 30),
        'order_payments': (10, 20, 30),
        'order_reviews': (2, 3, 4),
        'products': (100, 200, 300),
        'sellers': ('alpha', 'beta', 'gamma'),
    }

    def add_certification(table, batch, certification_time, business_time, value):
        key_cols = {
            'customers': ['customer_id'], 'orders': ['order_id'],
            'order_items': ['item_pk'],
            'order_payments': ['order_id', 'payment_sequential'],
            'order_reviews': ['review_pk'], 'products': ['product_id'],
            'sellers': ['seller_id'],
        }[table]
        data_cols = key_cols + [MODELS[table][1], 'updated_at', '_batch_id', '_source_file', '_row_hash']
        data = (*keys[table], value, business_time, batch, batch + '.parquet', batch + '-hash')
        assert len(data_cols) == len(data)
        with admin.get_conn() as conn, conn.cursor() as cur:
            cur.execute(
                '''INSERT INTO audit.landed_batches
                   (batch_id, table_name, extraction_mode, logical_interval_end,
                    parquet_key, manifest_key, row_count)
                   VALUES (%s,%s,'incremental',%s,%s,%s,1)''',
                (batch, table, business_time, batch + '.parquet', batch + '.json'),
            )
            cur.execute(
                '''INSERT INTO audit.batch_registry
                   (batch_id, table_name, status, expected_rows, actual_rows, certified_at)
                   VALUES (%s,%s,'CERTIFIED',1,1,%s)''',
                (batch, table, certification_time),
            )
            # Fixed allowlisted SQL identifiers; values remain parameters.
            cur.execute(
                f"INSERT INTO raw.{table} ({', '.join(data_cols)}) "
                f"VALUES ({', '.join(['%s'] * len(data))})", data,
            )

    def db():
        return psycopg.connect(host='test-db', port=5432, dbname='warehouse_test',
                               user='dbt_runner', password=os.environ['DBT_RUNNER_PASSWORD'])

    def snapshot():
        with db() as conn, conn.cursor() as cur:
            state = {}
            for table, (_, value_col) in MODELS.items():
                if table == 'order_payments':
                    where = 'order_id = %s AND payment_sequential = %s'
                else:
                    where = f'{MODELS[table][0]} = %s'
                cur.execute(f'SELECT {value_col}, source_batch_id FROM staging.stg_{table} '
                            f'WHERE {where}', keys[table])
                rows = cur.fetchall()
                assert len(rows) == 1, f'Incorrect grain for {table}: {rows}'
                state[table] = rows[0]
            return state

    def processed(model, batch):
        with db() as conn, conn.cursor() as cur:
            cur.execute('''SELECT COUNT(*) FROM audit.dbt_staging_processed_batches
                        WHERE model_name=%s AND batch_id=%s''', (f'stg_{model}', batch))
            return cur.fetchone()[0]

    def all_models(*extra, suffix='all'):
        invoke('run', '--select', *('stg_' + table for table in MODELS), *extra, suffix=suffix)

    def equivalent(actual, expected):
        from decimal import Decimal
        if isinstance(actual, Decimal):
            return actual == Decimal(str(expected))
        return str(actual).lower() == str(expected).lower()

    # Baseline batch for every source; tests exercise ALL seven incremental models.
    for table in MODELS:
        add_certification(table, prefix + '-' + table + '-base', base, base, values[table][0])
    all_models(suffix='baseline')
    baseline = snapshot()
    for table in MODELS:
        assert equivalent(baseline[table][0], values[table][0])

    # Late certification arrives later in real time, but its certified_at is EARLIER.
    # The new version has a later source business timestamp and must win.
    for table in MODELS:
        batch = prefix + '-' + table + '-late'
        add_certification(table, batch, base - timedelta(days=1),
                          base + timedelta(days=1), values[table][1])
    all_models(suffix='late')
    late = snapshot()
    for table in MODELS:
        assert equivalent(late[table][0], values[table][1])
        assert processed(table, prefix + '-' + table + '-late') == 1

    # Same certified_at, batch_id sorting BELOW prior high watermark.
    for table in MODELS:
        batch = 'a-' + prefix + '-' + table + '-tie'
        add_certification(table, batch, base, base + timedelta(days=2), values[table][2])
    all_models(suffix='tie')
    tied = snapshot()
    for table in MODELS:
        assert equivalent(tied[table][0], values[table][2])
        assert processed(table, 'a-' + prefix + '-' + table + '-tie') == 1

    # Failed post-hook must roll back merge AND processed-batch markers atomically.
    fail_batch = prefix + '-orders-fail'
    add_certification('orders', fail_batch, base + timedelta(days=3),
                      base + timedelta(days=3), 'shipped')
    invoke('run', '--select', 'stg_orders', '--vars', '{staging_test_fail_finalize: true}',
           expect_success=False, suffix='failure')
    assert snapshot()['orders'] == tied['orders'], 'Failed dbt transaction changed staging'
    assert processed('orders', fail_batch) == 0, 'Failed dbt transaction advanced ledger'
    invoke('run', '--select', 'stg_orders', suffix='recovery')
    assert snapshot()['orders'][0] == 'shipped'
    assert processed('orders', fail_batch) == 1

    # Two concurrent dbt processes on the same model must not lose any batch.
    concurrent_batch = prefix + '-orders-concurrent'
    add_certification('orders', concurrent_batch, base + timedelta(days=4),
                      base + timedelta(days=4), 'delivered')
    with ThreadPoolExecutor(max_workers=2) as executor:
        jobs = [executor.submit(invoke, 'run', '--select', 'stg_orders',
                                suffix='concurrent-' + str(i)) for i in range(2)]
        for job in jobs:
            job.result()
    assert snapshot()['orders'][0] == 'delivered'
    assert processed('orders', concurrent_batch) == 1

    # A full rebuild must match incremental current state for all 7 models.
    before = snapshot()
    all_models('--full-refresh', suffix='full-refresh')
    after = snapshot()
    assert before == after, 'Full refresh does not match incremental output'
    all_models(suffix='repeat')
    assert after == snapshot(), 'Repeated incremental run changed data'
