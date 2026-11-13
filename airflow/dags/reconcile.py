"""Daily source-vs-certified-raw drift detection, with explicit source quiescence."""

from datetime import datetime, timedelta, timezone

from airflow.sdk import dag, task
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor

from include.extract import MUTABLE_KEYS
from include.reconcile import compare_table


@dag(
    dag_id="reconcile",
    schedule="5 3 * * *",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["olist", "quality"],
)
def reconcile():
    wait_for_ingest = ExternalTaskSensor(
        task_id="wait_for_ingest",
        external_dag_id="ingest",
        execution_delta=timedelta(minutes=5),
        allowed_states=["success"],
        failed_states=["failed"],
        mode="reschedule",
        poke_interval=30,
        timeout=3600,
        queue="default",
    )

    @task(
        queue="batch",
        retries=1,
        retry_delay=timedelta(minutes=2),
        execution_timeout=timedelta(minutes=20),
    )
    def compare(table: str):
        return compare_table(table)

    comparisons = compare.expand(table=list(MUTABLE_KEYS))

    wait_for_ingest >> comparisons


reconcile()
