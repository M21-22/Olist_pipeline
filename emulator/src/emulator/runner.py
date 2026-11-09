"""Simulated clock and transactional event replay."""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone

import numpy as np

from .apply import Applier
from .db import get_state, set_state
from .defects import hit
from .events import Plan
from .settings import DefectConfig, Settings

log = logging.getLogger("emulator.run")


def _apply_tx(
    conn,
    settings: Settings,
    cfg: DefectConfig,
    applier: Applier,
    plan: Plan,
    i: int,
    e: int,
    delay: bool,
) -> None:
    """Apply events [i, e) and advance the cursor atomically."""

    with conn.transaction():
        with conn.cursor() as cur:
            for k in range(i, e):
                applier.apply(cur, k)

            sim = applier.sim_time(e - 1)

            set_state(
                cur,
                "cursor",
                {
                    "next_event": e,
                    "sim_time": sim.isoformat(),
                },
            )

            # Simulate a transaction whose commit is delayed.
            if delay and hit(cfg, "late_commit_tx", str(i)):
                seconds = float(cfg.params["late_commit_seconds"])

                applier.flush(
                    cur,
                    [
                        (
                            "late_commit",
                            "*",
                            {
                                "first_event": i,
                                "last_event": e - 1,
                            },
                            {"delay_seconds": seconds},
                        )
                    ],
                    sim,
                )

                time.sleep(seconds)


def run(
    conn,
    settings: Settings,
    cfg: DefectConfig,
    plan: Plan,
    applier: Applier,
    *,
    backfill: bool,
    max_events: int | None,
    stop,
) -> int:
    """Run or resume the event plan."""

    with conn.cursor() as cur:
        state = get_state(cur, "cursor") or {"next_event": 0}

    i = int(state["next_event"])
    n = len(plan)
    ts = plan.ts

    limit = n if max_events is None else min(n, i + max_events)

    sim_ns = int(ts[min(i, n - 1)]) if n else 0

    start_time = time.monotonic()
    last_log = start_time
    start_i = i

    mode = "backfill" if backfill else f"speed x{settings.sim_speed:g}"

    log.info(
        "Starting at event %d of %d (%s)",
        i,
        n,
        mode,
    )

    while i < limit and not stop.is_set():
        tick_start = time.monotonic()

        if backfill:
            j = limit
        else:
            sim_ns += int(settings.tick_seconds * settings.sim_speed * 1e9)

            j = min(
                limit,
                int(
                    np.searchsorted(
                        ts,
                        sim_ns,
                        side="right",
                    )
                ),
            )

        while i < j and not stop.is_set():
            e = min(
                i + settings.max_events_per_tx,
                j,
            )

            _apply_tx(
                conn,
                settings,
                cfg,
                applier,
                plan,
                i,
                e,
                delay=not backfill,
            )

            i = e

        if time.monotonic() - last_log >= 10:
            last_log = time.monotonic()

            sim_date = (
                datetime.fromtimestamp(
                    int(ts[i - 1]) / 1e9,
                    tz=timezone.utc,
                ).date()
                if i
                else "-"
            )

            elapsed = time.monotonic() - start_time
            rate = (i - start_i) / max(
                1e-9,
                elapsed,
            )

            log.info(
                "Event %d/%d | sim date %s | %.0f ev/s | defects %d",
                i,
                n,
                sim_date,
                rate,
                sum(applier.stats.values()),
            )

        if not backfill and i < limit:
            elapsed_tick = time.monotonic() - tick_start

            time.sleep(
                max(
                    0.0,
                    settings.tick_seconds - elapsed_tick,
                )
            )

    log.info(
        "Stopped at event %d of %d; defects injected this run: %s",
        i,
        n,
        dict(applier.stats),
    )

    return i
