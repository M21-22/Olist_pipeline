"""Dependency-free source contract checks for Package D DAG structure.

These are lightweight static checks; validate actual Airflow DAG parsing in the
running Airflow 3 image separately before scheduling production tasks.
"""

import ast
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
AIRFLOW_ROOT = REPO / "airflow" if (REPO / "airflow" / "dags").is_dir() else REPO
DAGS = AIRFLOW_ROOT / "dags"


def source(name):
    return (DAGS / name).read_text(encoding="utf-8")


def test_ingest_uses_mapped_group_with_per_table_dependency():
    text = source("ingest.py")
    tree = ast.parse(text)
    assert 'per_table.expand(table=mutable_tables())' in text
    assert 'certify(land(table))' in text
    assert not any('certify.expand(' in line for line in text.splitlines())
    assert any(
        isinstance(node, ast.FunctionDef)
        and node.name == 'per_table'
        and any(
            (isinstance(d, ast.Call) and isinstance(d.func, ast.Name) and d.func.id == 'task_group')
            or (isinstance(d, ast.Name) and d.id == 'task_group')
            for d in node.decorator_list
        )
        for node in ast.walk(tree)
    )


def test_heavy_imports_are_not_at_dag_module_scope():
    for name in ('ingest.py', 'reconcile.py'):
        tree = ast.parse(source(name))
        imports = [node for node in tree.body if isinstance(node, (ast.Import, ast.ImportFrom))]
        for node in imports:
            if isinstance(node, ast.ImportFrom):
                assert not node.module.startswith('include.')
            else:
                assert all(not alias.name.startswith(('include.', 'pyarrow', 'duckdb')) for alias in node.names)


def test_ingest_keeps_scheduling_and_queues():
    text = source('ingest.py')
    assert 'schedule="*/15 * * * *"' in text
    assert 'max_active_runs=1' in text
    assert text.count('queue="ingest"') == 2
    assert 'except Gate1Failure' in text
    assert 'raise AirflowFailException' in text
    assert 'retries=3' in text


def test_reconcile_preserves_dependency_and_batch_queue():
    text = source('reconcile.py')
    assert 'wait_for_ingest >> tables' in text
    assert 'compare.expand(table=tables)' in text
    assert 'queue="batch"' in text
    assert 'mode="continuous"' in text
