"""Database connections, migrations, and emulator state."""

from __future__ import annotations

import logging
from pathlib import Path

import psycopg
from psycopg.types.json import Jsonb

from .settings import Settings

log = logging.getLogger("emulator.db")


def connect(settings: Settings) -> psycopg.Connection:
    return psycopg.connect(settings.conninfo(), autocommit=True)


def migrate(conn: psycopg.Connection, sql_dir: Path) -> int:
    applied = 0

    with conn.cursor() as cur:
        cur.execute(
            "CREATE TABLE IF NOT EXISTS chaos.migrations ("
            "version text PRIMARY KEY, "
            "applied_at timestamptz NOT NULL DEFAULT now())"
        )

        done = {
            row[0]
            for row in cur.execute("SELECT version FROM chaos.migrations").fetchall()
        }

        for file in sorted(Path(sql_dir).glob("*.sql")):
            if file.name in done:
                continue

            with conn.transaction():
                cur.execute(file.read_text())
                cur.execute(
                    "INSERT INTO chaos.migrations (version) VALUES (%s)",
                    (file.name,),
                )

            log.info("applied migration %s", file.name)
            applied += 1

    return applied


def get_state(cur, key: str):
    cur.execute(
        "SELECT value FROM chaos.emulator_state WHERE key = %s",
        (key,),
    )
    row = cur.fetchone()
    return row[0] if row else None


def set_state(cur, key: str, value) -> None:
    cur.execute(
        "INSERT INTO chaos.emulator_state (key, value) "
        "VALUES (%s, %s) "
        "ON CONFLICT (key) DO UPDATE "
        "SET value = EXCLUDED.value, updated_at = now()",
        (key, Jsonb(value)),
    )
