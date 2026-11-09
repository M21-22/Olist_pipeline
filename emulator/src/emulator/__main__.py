"""CLI entry point: python -m emulator <command>."""

from __future__ import annotations

import argparse
import logging
import os
import signal
import threading
from pathlib import Path

from .apply import Applier
from .data import load_all
from .db import connect, get_state, migrate
from .events import build
from .profile import profile
from .runner import run
from .seed import seed
from .settings import DefectConfig, Settings
from .verify import fingerprint, status, verify

SQL_DIR = Path(os.environ.get("SQL_DIR", "/app/sql"))
log = logging.getLogger("emulator")


def plan_id(cfg, start, end):
    return {
        "digest": cfg.digest(),
        "from": start,
        "to": end,
    }


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="emulator")
    commands = parser.add_subparsers(dest="command", required=True)

    for name in (
        "migrate",
        "status",
        "verify",
        "fingerprint",
        "reset",
        "profile",
    ):
        commands.add_parser(name)

    for name in ("seed", "run"):
        cmd = commands.add_parser(name)
        cmd.add_argument(
            "--from-date",
            default=os.environ.get("SIM_FROM") or None,
        )
        cmd.add_argument(
            "--to-date",
            default=os.environ.get("SIM_TO") or None,
        )

    commands.choices["run"].add_argument("--backfill", action="store_true")
    commands.choices["run"].add_argument("--max-events", type=int, default=None)

    args = parser.parse_args(argv)
    settings = Settings.from_env()

    logging.basicConfig(
        level=settings.log_level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    if args.command == "profile":
        profile(settings.data_dir)
        return 0

    cfg = DefectConfig.load(settings.defects_file)

    with connect(settings) as conn:
        if args.command == "migrate":
            count = migrate(conn, SQL_DIR)
            print(f"{count} migration(s) applied")
            return 0

        if args.command == "reset":
            with conn.cursor() as cur:
                cur.execute(
                    "TRUNCATE "
                    "shop.order_reviews, "
                    "shop.order_payments, "
                    "shop.order_items, "
                    "shop.orders, "
                    "shop.customers, "
                    "shop.products, "
                    "shop.sellers, "
                    "shop.geolocation, "
                    "shop.product_category_translation, "
                    "chaos.injected_defects, "
                    "chaos.emulator_state "
                    "RESTART IDENTITY"
                )
            print("Source data and emulator state cleared")
            return 0

        if args.command == "status":
            status(conn)
            return 0

        if args.command == "fingerprint":
            fingerprint(conn)
            return 0

        if args.command == "verify":
            with conn.cursor() as cur:
                stored = get_state(cur, "plan")

            if not stored:
                raise SystemExit("Nothing seeded yet.")

            data = load_all(settings.data_dir)
            plan = build(data, cfg, stored["from"], stored["to"])
            return 0 if verify(conn, settings, cfg, data, plan) else 1

        # Seed or run the simulation.
        identity = plan_id(cfg, args.from_date, args.to_date)
        data = load_all(settings.data_dir)
        plan = build(data, cfg, args.from_date, args.to_date)

        log.info(
            "Plan: %d events for %d orders",
            len(plan),
            len(plan.selected),
        )

        if args.command == "seed":
            migrate(conn, SQL_DIR)
            seed(conn, settings, cfg, data, plan, identity)
            return 0

        with conn.cursor() as cur:
            seeded = get_state(cur, "seeded")
            stored = get_state(cur, "plan")

        if not seeded:
            raise SystemExit("Not seeded. Run `seed` first.")

        if stored != identity:
            raise SystemExit(
                "Plan mismatch. Use the original date range and seed, or run `reset`."
            )

        stop = threading.Event()
        signal.signal(
            signal.SIGTERM,
            lambda *_: stop.set(),
        )
        signal.signal(
            signal.SIGINT,
            lambda *_: stop.set(),
        )

        applier = Applier(cfg, data, plan)

        end = run(
            conn,
            settings,
            cfg,
            plan,
            applier,
            backfill=args.backfill,
            max_events=args.max_events,
            stop=stop,
        )

        if end >= len(plan):
            log.info("Simulation complete")

        return 0


if __name__ == "__main__":
    raise SystemExit(main())
