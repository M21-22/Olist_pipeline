"""Keep both transformation entry points on the shared publication path."""

import ast
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
AIRFLOW = REPO / "airflow" if (REPO / "airflow" / "dags").exists() else REPO


def test_transformation_dags_use_batch_queue_and_explicit_publish_mode():
    for name, publish in (("transform_staging", False), ("transform_marts", True)):
        source = (AIRFLOW / "dags" / f"{name}.py").read_text(encoding="utf-8")
        ast.parse(source)
        assert f'dag_id="{name}"' in source
        schedule = 'os.getenv("OLIST_TRANSFORM_MARTS_SCHEDULE") or None' if publish else 'None'
        assert f'schedule={schedule}' in source
        assert 'max_active_runs=1' in source
        assert 'queue="batch"' in source
        assert f'run_transform(conn, run_id, publish={publish})' in source
