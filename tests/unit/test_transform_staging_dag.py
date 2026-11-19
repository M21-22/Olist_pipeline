"""Static safeguards for the manual staging-only transformation DAG.

Runtime import and a real Airflow task run must be verified in the Airflow image.
"""

import ast
from pathlib import Path


REPO = Path(__file__).resolve().parents[2]
DAG = (REPO / 'airflow' / 'dags' / 'transform_staging.py')
if not DAG.exists():
    DAG = REPO / 'dags' / 'transform_staging.py'


def _source():
    return DAG.read_text(encoding='utf-8')


def test_transform_staging_is_manual_and_not_publishing():
    src = _source()
    ast.parse(src)
    assert 'dag_id="transform_staging"' in src
    assert 'schedule=None' in src
    assert 'max_active_runs=1' in src
    assert 'serving_state' not in src
    assert 'fct_orders' not in src


def test_batch_queue_and_validation_gate_before_sync():
    src = _source()
    assert src.count('queue="batch"') == 3
    assert 'build >> validate >> sync_rejections' in src
    assert 'task_id="gate3_validate_staging"' in src
    assert '"gate3"' in src
    assert 'path:models/staging rejected_rows_candidates' in src
    assert 'path:models/staging tag:staging_contract' in src
    assert 'run-operation sync_rejected_rows' in src
    assert src.count('set -euo pipefail') == 3


def test_dbt_runtime_uses_read_only_project_with_writable_artifacts():
    src = _source()
    assert '/home/airflow/dbt-venv/bin/dbt' in src
    assert '--project-dir /opt/airflow/dbt' in src
    assert '--target-path /tmp/' in src
    assert '--log-path /tmp/' in src
    assert '--full-refresh' not in src
