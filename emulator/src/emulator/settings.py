"""Runtime settings and defect configuration."""

from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass
from pathlib import Path

import yaml


@dataclass(frozen=True)
class Settings:
    db_host: str
    db_port: int
    db_name: str
    db_user: str
    db_password: str
    data_dir: Path
    defects_file: Path
    sim_speed: float
    tick_seconds: float
    max_events_per_tx: int
    log_level: str

    @classmethod
    def from_env(cls) -> Settings:
        env = os.environ
        return cls(
            db_host=env.get("DB_HOST", "source-db"),
            db_port=int(env.get("DB_PORT", "5432")),
            db_name=env.get("DB_NAME", "source"),
            db_user=env.get("DB_USER", "emulator_writer"),
            db_password=env["DB_PASSWORD"],
            data_dir=Path(env.get("DATA_DIR", "/data")),
            defects_file=Path(env.get("DEFECTS_FILE", "/app/config/defects.yaml")),
            sim_speed=float(env.get("SIM_SPEED") or "1440"),
            tick_seconds=float(env.get("TICK_SECONDS") or "5"),
            max_events_per_tx=int(env.get("MAX_EVENTS_PER_TX") or "50"),
            log_level=env.get("LOG_LEVEL", "INFO"),
        )

    def conninfo(self) -> str:
        return (
            f"host={self.db_host} port={self.db_port} "
            f"dbname={self.db_name} user={self.db_user} "
            f"password={self.db_password} "
            "options='-c timezone=UTC' application_name=emulator"
        )


@dataclass(frozen=True)
class DefectConfig:
    seed: int
    enabled: bool
    rates: dict
    params: dict

    @classmethod
    def load(cls, path: Path) -> DefectConfig:
        config = yaml.safe_load(path.read_text())
        return cls(
            seed=int(os.environ.get("SEED") or config["seed"]),
            enabled=bool(config.get("enabled", True)),
            rates=dict(config["rates"]),
            params=dict(config["params"]),
        )

    def rate(self, name: str) -> float:
        return float(self.rates.get(name, 0.0)) if self.enabled else 0.0

    def lazy_fraction(self) -> float:
        if not self.enabled:
            return 0.0
        return float(self.params.get("lazy_master_fraction", 0.0))

    def digest(self) -> str:
        payload = json.dumps(
            {
                "seed": self.seed,
                "enabled": self.enabled,
                "rates": self.rates,
                "params": self.params,
            },
            sort_keys=True,
        )
        return hashlib.sha256(payload.encode()).hexdigest()[:16]
