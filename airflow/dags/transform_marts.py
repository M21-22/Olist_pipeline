"""Build staging, marts and API views, then publish through Gates 3–4."""

import os
from datetime import UTC, datetime, timedelta

from airflow.sdk import dag, get_current_context, task


@dag(
    dag_id="transform_marts",
    schedule=os.getenv("OLIST_TRANSFORM_MARTS_SCHEDULE") or None,
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    tags=["olist", "dbt", "marts", "api", "gate4"],
)
def transform_marts():
    @task(queue="batch", retries=0, execution_timeout=timedelta(minutes=90))
    def validate_and_publish():
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        from include.transform import run_transform

        context = get_current_context()
        run_id = f"{context['dag'].dag_id}/{context['run_id']}/{context['ti'].try_number}"
        conn = PostgresHook(postgres_conn_id="olist_reconcile").get_conn()
        run_transform(conn, run_id, publish=True)

    validate_and_publish()


transform_marts()
