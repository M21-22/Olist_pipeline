"""Incremental landing and basic raw certification. Two distinct steps per table."""

from datetime import datetime, timedelta, timezone

from airflow.sdk import dag, task, get_current_context

from include.extract import MUTABLE_KEYS, land_table
from include.load import load_batch


@dag(
    dag_id="ingest",
    schedule="*/15 * * * *",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["olist", "ingestion"],
)
def ingest():
    @task(
        queue="ingest",
        retries=3,
        retry_delay=timedelta(minutes=1),
        retry_exponential_backoff=True,
        execution_timeout=timedelta(minutes=10),
    )
    def land(table: str):
        interval_end = get_current_context()["data_interval_end"].isoformat()
        return land_table(table, interval_end)

    @task(
        queue="ingest",
        retries=3,
        retry_delay=timedelta(minutes=1),
        retry_exponential_backoff=True,
        execution_timeout=timedelta(minutes=10),
    )
    def certify(batch: str):
        return load_batch(batch)

    landed = land.expand(table=list(MUTABLE_KEYS))
    certify.expand(batch=landed)


ingest()
