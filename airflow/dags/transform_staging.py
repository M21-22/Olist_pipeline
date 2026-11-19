"""Manual staging build, Gate 3 validation, and rejection synchronization.

Runs on the batch Celery queue; marts and serving publication are separate.
"""

from datetime import datetime, timedelta, timezone

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag

DBT = "/home/airflow/dbt-venv/bin/dbt"
DBT_ARGS = (
    "--project-dir /opt/airflow/dbt "
    "--profiles-dir /opt/airflow/dbt "
    "--target-path /tmp/olist-transform-staging-target "
    "--log-path /tmp/olist-transform-staging-logs"
)


@dag(
    dag_id="transform_staging",
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["olist", "dbt", "staging", "gate3"],
)
def transform_staging():
    build = BashOperator(
        task_id="build_staging",
        bash_command=(
            f"set -euo pipefail; {DBT} run {DBT_ARGS} "
            "--select path:models/staging rejected_rows_candidates"
        ),
        queue="batch",
        retries=1,
        retry_delay=timedelta(minutes=1),
        execution_timeout=timedelta(minutes=30),
    )

    validate = BashOperator(
        task_id="gate3_validate_staging",
        bash_command=(
            f"set -euo pipefail; {DBT} test {DBT_ARGS} "
            "--select path:models/staging tag:staging_contract"
        ),
        queue="batch",
        retries=0,
        execution_timeout=timedelta(minutes=15),
    )

    sync_rejections = BashOperator(
        task_id="sync_rejected_rows",
        bash_command=(
            f"set -euo pipefail; {DBT} run-operation sync_rejected_rows {DBT_ARGS}"
        ),
        queue="batch",
        retries=1,
        retry_delay=timedelta(minutes=1),
        execution_timeout=timedelta(minutes=10),
    )

    build >> validate >> sync_rejections


transform_staging()
