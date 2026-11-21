"""Build the customer snapshot, dimensional marts, and API contract views.

Run after a successful transform_staging DAG. Deliberately manual until the
end-to-end publishing policy is introduced; avoid serving partially validated
staging data on an independent schedule.

Test execution policy (each test runs once per DAG run):
  * build    - models plus generic (YAML) tests; singular and unit tests excluded.
  * validate - singular data tests only.
  * dbt unit tests run in CI, not here (they use fixtures, not warehouse data).
"""

from datetime import datetime, timedelta, timezone

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag

DBT = "/home/airflow/dbt-venv/bin/dbt"
DBT_ARGS = (
    "--project-dir /opt/airflow/dbt "
    "--profiles-dir /opt/airflow/dbt "
    "--target-path /tmp/olist-transform-marts-target "
    "--log-path /tmp/olist-transform-marts-logs"
)


@dag(
    dag_id="transform_marts",
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["olist", "dbt", "marts", "api"],
)
def transform_marts():
    snapshot = BashOperator(
        task_id="snapshot_customer_history",
        bash_command=(
            f"set -euo pipefail; {DBT} snapshot {DBT_ARGS} "
            "--select customer_history"
        ),
        queue="batch",
        retries=1,
        retry_delay=timedelta(minutes=1),
        execution_timeout=timedelta(minutes=20),
    )

    build = BashOperator(
        task_id="build_marts_and_api",
        bash_command=(
            f"set -euo pipefail; {DBT} build {DBT_ARGS} "
            "--select path:models/marts path:models/api "
            "--exclude 'test_type:singular' 'test_type:unit'"
        ),
        queue="batch",
        # A failed data test is not transient; retrying would rebuild every model.
        retries=0,
        execution_timeout=timedelta(minutes=45),
    )

    validate = BashOperator(
        task_id="validate_marts_and_api",
        bash_command=(
            f"set -euo pipefail; {DBT} test {DBT_ARGS} "
            "--select 'assert_dim_*' 'assert_dimension_*' "
            "'assert_fact_*' 'assert_mart_*' 'assert_api_*'"
        ),
        queue="batch",
        retries=0,
        execution_timeout=timedelta(minutes=30),
    )

    snapshot >> build >> validate


transform_marts()
