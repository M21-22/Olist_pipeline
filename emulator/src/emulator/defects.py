"""Deterministic randomness and value-level defect injection."""

from __future__ import annotations
import hashlib
from .settings import DefectConfig
from datetime import datetime, timezone

BAD_STATES = ["XX", "ZZ", "00", "S P"]
BAD_STATUSES = ["teleported", "lost", "??"]
BAD_SCORES = ["0", "6", "9", "-1"]
FAR_FUTURE = datetime(2099, 1, 1, tzinfo=timezone.utc)


def u01(seed: int, salt: str, key: str) -> float:
    """Return a stable number in [0, 1) for a seed, salt and key."""
    payload = f"{seed}|{salt}|{key}".encode()
    digest = hashlib.blake2b(payload, digest_size=8).digest()
    return int.from_bytes(digest, "big") / 2**64


def hit(cfg: DefectConfig, name: str, key: str) -> bool:
    rate = cfg.rate(name)
    return rate > 0 and u01(cfg.seed, name, key) < rate


def uniform(
    cfg: DefectConfig,
    salt: str,
    key: str,
    lo: float,
    hi: float,
) -> float:
    return lo + (hi - lo) * u01(cfg.seed, salt, key)


def choose(
    cfg: DefectConfig,
    salt: str,
    key: str,
    options: list,
):
    index = int(u01(cfg.seed, salt, key) * len(options))
    return options[index]


def _noisy(cfg: DefectConfig, key: str, value: str) -> str:
    kind = int(u01(cfg.seed, "noise_kind", key) * 4)

    if kind == 0:
        return " " + value
    if kind == 1:
        return value + "  "
    if kind == 2:
        return value.upper() if value != value.upper() else value.lower()

    return value.swapcase()


class Defects:
    """Apply deterministic defects to source master records."""

    def __init__(self, cfg: DefectConfig):
        self.cfg = cfg

    def _invalid_state(
        self,
        table: str,
        key_col: str,
        row: dict,
        state_col: str,
        logs: list,
    ) -> None:
        key = f"{table}|{row[key_col]}"

        if row.get(state_col) and hit(self.cfg, "invalid_state_code", key):
            original = row[state_col]
            row[state_col] = choose(self.cfg, "bad_state", key, BAD_STATES)
            logs.append(
                (
                    "invalid_state_code",
                    table,
                    {key_col: row[key_col]},
                    {
                        "column": state_col,
                        "original": original,
                        "value": row[state_col],
                    },
                )
            )

    def _noise(
        self,
        table: str,
        key_col: str,
        row: dict,
        fields: list,
        logs: list,
    ) -> None:
        key = f"{table}|{row[key_col]}"

        if not hit(self.cfg, "text_noise", key):
            return

        field = choose(self.cfg, "noise_field", key, fields)
        original = row.get(field)

        if not original:
            return

        new_value = _noisy(self.cfg, key, original)

        if new_value != original:
            row[field] = new_value
            logs.append(
                (
                    "text_noise",
                    table,
                    {key_col: row[key_col]},
                    {
                        "column": field,
                        "original": original,
                        "value": new_value,
                    },
                )
            )

    def customer(self, row: dict):
        logs = []
        self._invalid_state(
            "customers",
            "customer_id",
            row,
            "customer_state",
            logs,
        )
        self._noise(
            "customers",
            "customer_id",
            row,
            ["customer_city", "customer_state"],
            logs,
        )
        return row, logs

    def seller(self, row: dict):
        logs = []
        self._invalid_state(
            "sellers",
            "seller_id",
            row,
            "seller_state",
            logs,
        )
        self._noise(
            "sellers",
            "seller_id",
            row,
            ["seller_city", "seller_state"],
            logs,
        )
        return row, logs

    def product(self, row: dict):
        logs = []
        self._noise(
            "products",
            "product_id",
            row,
            ["product_category_name"],
            logs,
        )
        return row, logs
