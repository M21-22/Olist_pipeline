"""Independent per-table landing and certification (Airflow 3 mapped TaskGroup)."""

from datetime import datetime, timedelta, timezone

from airflow.sdk import dag, get_current_context, task, task_group
from airflow.sdk.exceptions import AirflowFailException


@dag(
    dag_id="ingest",
    schedule="*/15 * * * *",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["olist", "ingestion"],
)
def ingest():
    @task(queue="default")
    def mutable_tables() -> list[str]:
        from include.common import MUTABLE_KEYS

        return list(MUTABLE_KEYS)

    @task_group(group_id="per_table")
    def per_table(table: str):
        @task(
            queue="ingest",
            retries=3,
            retry_delay=timedelta(minutes=1),
            retry_exponential_backoff=True,
            execution_timeout=timedelta(minutes=10),
        )
        def land(table: str) -> str:
            from include.extract import land_table

            context = get_current_context()
            interval_end = context.get("data_interval_end")
            if interval_end is None:
                interval_end = context["dag_run"].run_after

            return land_table(table, interval_end.isoformat())

        @task(
            queue="ingest",
            retries=3,
            retry_delay=timedelta(minutes=1),
            retry_exponential_backoff=True,
            execution_timeout=timedelta(minutes=10),
        )
        def certify(batch: str):
            from include.gates.gate1 import Gate1Failure
            from include.load import load_batch

            try:
                return load_batch(batch)
            except Gate1Failure as error:
                raise AirflowFailException(
                    f"Gate 1 validation failed: {error}"
                ) from error

        certify(land(table))

    per_table.expand(table=mutable_tables())


ingest()
