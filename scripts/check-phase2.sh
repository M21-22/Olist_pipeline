#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

export SIM_FROM=2017-03-01
export SIM_TO=2017-03-08

COMPOSE=(docker compose -f infra/compose.yaml)
EMU=("${COMPOSE[@]}" --profile emulator run --rm -T emulator)

reset_emulator() {
    "${EMU[@]}" reset
}

replay() {
    "${EMU[@]}" seed
    "${EMU[@]}" run --backfill
    "${EMU[@]}" verify
}

echo "== First run: interrupted and resumed =="

reset_emulator
"${EMU[@]}" seed
"${EMU[@]}" run --backfill --max-events 400
"${EMU[@]}" run --backfill
"${EMU[@]}" verify

"${EMU[@]}" fingerprint > /tmp/olist-phase2-first.txt

echo "== Second run: uninterrupted =="

reset_emulator
replay

"${EMU[@]}" fingerprint > /tmp/olist-phase2-second.txt

echo "== Comparing fingerprints =="

diff -u /tmp/olist-phase2-first.txt /tmp/olist-phase2-second.txt

echo "PASS: Phase 2 replay and reproducibility"
