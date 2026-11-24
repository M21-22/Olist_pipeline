"""Run the dbt and Soda gates within one serialized publication attempt."""

import json
import os
import subprocess
from contextlib import closing, contextmanager
from pathlib import Path
from tempfile import TemporaryDirectory

DBT = "/home/airflow/dbt-venv/bin/dbt"
PROJECT = "/opt/airflow/dbt"
SODA = "/home/airflow/soda-venv/bin/python"
SODA_SCAN = "/opt/airflow/soda/scan_gate4.py"

STAGING_STEPS = (
    ("staging_build", "build", "--select", "path:models/staging", "rejected_rows_candidates",
     "tag:staging_contract", "--indirect-selection", "cautious", "--exclude", "test_type:unit"),
    ("sync_rejections", "run-operation", "sync_rejected_rows"),
)
MART_STEPS = (
    ("marts_build", "build", "--select", "path:models/marts", "path:models/api",
     "--exclude", "test_type:unit"),
)


@contextmanager
def _transaction_cursor(conn):
    # psycopg 3 closes the session on leaving a connection context.
    try:
        with conn.cursor() as cursor:
            yield cursor
        conn.commit()
    except BaseException:
        conn.rollback()
        raise


def _record_results(conn, run_id, gate, step, checks):
    with _transaction_cursor(conn) as cursor:
        cursor.executemany(
            """
            INSERT INTO audit.dq_results
                (batch_id, table_name, gate, check_name, status, failed_count, details)
            VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb)
            ON CONFLICT (batch_id, gate, check_name) DO UPDATE SET
                status = EXCLUDED.status, failed_count = EXCLUDED.failed_count,
                details = EXCLUDED.details, checked_at = clock_timestamp()
            """,
            [
                (run_id, dataset, gate, f"{step}/{name}", status, failures,
                 json.dumps({"run_id": run_id, "step": step}))
                for dataset, name, status, failures in checks
            ],
        )


def _dbt_step(conn, run_id, gate, directory, step, command, *selection):
    target = directory / step
    result = subprocess.run(
        [DBT, command, *selection, "--project-dir", PROJECT, "--profiles-dir", PROJECT,
         "--target-path", str(target), "--log-path", str(directory / "logs")],
        check=False,
        env={
            name: os.environ[name]
            for name in ("PATH", "LANG", "LC_ALL", "DBT_HOST", "DBT_RUNNER_PASSWORD")
            if name in os.environ
        },
    )
    artifact = target / "run_results.json"
    checks = []
    if artifact.exists():
        for entry in json.loads(artifact.read_text())["results"]:
            unique_id = entry["unique_id"]
            if unique_id.startswith("test.") or entry["status"] == "error":
                status = {
                    "pass": "PASS", "warn": "WARN", "fail": "FAIL", "skipped": "SKIPPED",
                }.get(entry["status"], "ERROR")
                checks.append(("dbt", unique_id, status, entry.get("failures") or 0))
    checks.append(("dbt", "execution", "FAIL" if result.returncode else "PASS", 0))
    _record_results(conn, run_id, gate, step, checks)
    result.check_returncode()
    return any(check[2] == "WARN" for check in checks)


def _soda_step(conn, run_id, directory):
    result_path = directory / "soda-results.json"
    result = subprocess.run(
        [SODA, SODA_SCAN, str(result_path)], check=False,
        env={
            **{name: os.environ[name] for name in ("PATH", "SODA_READER_PASSWORD")},
            "SODA_HOST": os.getenv("DBT_HOST", "warehouse-db"),
        },
    )
    checks = [
        (entry["table"] or "warehouse", f"{entry['name']} [{entry['identity']}]", entry["outcome"].upper(),
         int(entry["outcome"] in {"warn", "fail"}))
        for entry in json.loads(result_path.read_text()) if entry["outcome"]
    ] if result_path.exists() else []
    passed = result.returncode in (0, 1) and bool(checks)
    checks.append(("warehouse", "execution", "PASS" if passed else "FAIL", 0))
    _record_results(conn, run_id, "GATE_4", "soda", checks)
    if not passed:
        raise RuntimeError(f"Soda Gate 4 scan failed (exit {result.returncode})")
    return result.returncode == 1


def _set_state(conn, status, reason, *, successful=False, baseline_eligible=False):
    with _transaction_cursor(conn) as cursor:
        cursor.execute(
            """UPDATE audit.serving_state SET status = %s, reason = %s,
                  updated_at = clock_timestamp(),
                  last_successful_transform_at = CASE WHEN %s THEN clock_timestamp()
                      ELSE last_successful_transform_at END
               WHERE state_id = 1""",
            (status, reason, successful),
        )
        if cursor.rowcount != 1:
            raise RuntimeError("Serving state row is missing")
        if successful:
            cursor.execute(
                """INSERT INTO audit.metrics_history
                    (metric_name, object_name, measured_at, metric_value,
                     source_interval_end, baseline_eligible)
                   SELECT metric_name, object_name, CURRENT_TIMESTAMP, metric_value,
                          source_interval_end, %s
                   FROM marts.gate4_metrics WHERE metric_value IS NOT NULL""",
                (baseline_eligible,),
            )


def run_transform(conn, run_id, *, publish):
    """Hold the warehouse lock across subprocesses and release it on disconnect."""
    with closing(conn), TemporaryDirectory(prefix="olist-transform-") as temp:
        with _transaction_cursor(conn) as cursor:
            cursor.execute("SELECT pg_advisory_lock(hashtextextended('olist:transform', 0))")
        directory = Path(temp)
        step = "publication_start"
        if publish:
            # Block readers before any dbt model commits. A killed worker leaves the gate closed.
            _set_state(conn, "BLOCKED", f"Transform in progress: {run_id}")
        try:
            warned = False
            for gate, steps in (("GATE_3", STAGING_STEPS), ("GATE_4", MART_STEPS if publish else ())):
                for step, *args in steps:
                    warned |= _dbt_step(conn, run_id, gate, directory, step, *args)
            if publish:
                step = "soda"
                soda_warned = _soda_step(conn, run_id, directory)
                warned |= soda_warned
                step = "publication"
                status = "DEGRADED" if warned else "HEALTHY"
                reason = f"Quality warnings: {run_id}" if warned else None
                _set_state(conn, status, reason, successful=True, baseline_eligible=not soda_warned)
        except Exception:
            _set_state(conn, "BLOCKED", f"Failed {step}: {run_id}")
            raise
