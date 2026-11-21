"""Verify repeated-build equivalence over all nine staging models in disposable DB.

Runs once with whatever certified fixtures already exist in the isolated DB; this
verifies output stability, not every late-certification/concurrency edge case.
"""
import os
import subprocess
from pathlib import Path

import psycopg
import pytest
from airflow.providers.postgres.hooks.postgres import PostgresHook

if os.environ.get('OLIST_RUN_DB_INTEGRATION') != '1' or os.environ.get('OLIST_TEST_DBT') != '1':
    pytest.skip('Disposable dbt integration disabled', allow_module_level=True)

TABLES = ('stg_customers','stg_orders','stg_order_items','stg_order_payments',
          'stg_order_reviews','stg_products','stg_sellers','stg_geolocation',
          'stg_product_category_translation')

@pytest.mark.integration
def test_all_staging_models_repeat_without_data_changes(tmp_path: Path):
    admin = PostgresHook(postgres_conn_id='olist_test_admin')
    with admin.get_conn() as conn, conn.cursor() as cur:
        cur.execute('SELECT current_database()')
        assert cur.fetchone()[0] == 'warehouse_test', 'Refusing non-test database'

    (tmp_path / 'profiles.yml').write_text(
        'olist:\n  target: dev\n  outputs:\n    dev:\n      type: postgres\n'
        '      host: test-db\n      port: 5432\n      dbname: warehouse_test\n'
        '      user: dbt_runner\n'
        '      password: "{{ env_var(\'DBT_RUNNER_PASSWORD\') }}"\n'
        '      schema: staging\n      threads: 1\n'
    )

    def invoke(*args):
        process = subprocess.run(
            ['/home/airflow/dbt-venv/bin/dbt', *args,
             '--project-dir', '/opt/airflow/dbt', '--profiles-dir', str(tmp_path),
             '--target-path', str(tmp_path / 'target'),
             '--log-path', str(tmp_path / 'logs')],
            text=True, capture_output=True, timeout=240, check=False,
            env=os.environ.copy(),
        )
        assert process.returncode == 0, process.stdout[-6000:] + '\n' + process.stderr[-2000:]

    def snapshot():
        result = {}
        with psycopg.connect(host='test-db', dbname='warehouse_test',
                             user='dbt_runner', password=os.environ['DBT_RUNNER_PASSWORD']) as conn:
            with conn.cursor() as cur:
                for table in TABLES:
                    # Table names come from a fixed allow-list, never user input.
                    cur.execute(f'SELECT row_to_json(t)::text FROM staging.{table} AS t')
                    result[table] = sorted(row[0] for row in cur.fetchall())
                cur.execute('SELECT model_name,last_certified_at,last_batch_id FROM audit.dbt_staging_progress ORDER BY model_name')
                result['cursors'] = cur.fetchall()
        return result

    def assert_provenance():
        failures = []

        with psycopg.connect(
            host="test-db",
            dbname="warehouse_test",
            user="dbt_runner",
            password=os.environ["DBT_RUNNER_PASSWORD"],
        ) as conn:
            with conn.cursor() as cur:
                for model in TABLES:
                    source_table = model.removeprefix("stg_")
                    cur.execute(
                        f"""
                        SELECT s.source_batch_id, b.status, b.table_name
                        FROM staging.{model} AS s
                        LEFT JOIN audit.batch_registry AS b
                            ON b.batch_id = s.source_batch_id
                        AND b.table_name = %s
                        AND b.status = 'CERTIFIED'
                        WHERE b.batch_id IS NULL
                        """,
                        (source_table,),
                    )
                    failures.extend(
                        (model, *row) for row in cur.fetchall()
                    )

        assert not failures, f"Uncertified staging rows: {failures}"

    invoke('run','--select','path:models/staging')
    before = snapshot()
    invoke('run','--select','path:models/staging')
    after = snapshot()
    assert before == after, 'Staging data/certification cursor changed without new batches'

    assert_provenance()

    invoke(
        'test',
        '--select', 'path:models/staging',
        '--exclude', 'test_type:singular', 'test_type:unit',
    )
