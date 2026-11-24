"""Manual staging build, Gate 3 validation and rejection synchronization."""

from datetime import UTC, datetime, timedelta

from airflow.sdk import dag, get_current_context, task


@dag(
    dag_id="transform_staging",
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    tags=["olist", "dbt", "staging", "gate3"],
)
def transform_staging():
    @task(queue="batch", retries=0, execution_timeout=timedelta(minutes=45))
    def validate_staging():
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        from include.transform import run_transform

        context = get_current_context()
        run_id = f"{context['dag'].dag_id}/{context['run_id']}/{context['ti'].try_number}"
        conn = PostgresHook(postgres_conn_id="olist_reconcile").get_conn()
        run_transform(conn, run_id, publish=False)

    validate_staging()


transform_staging()
