"""Settings: secrets from .env, everything else from config/universe.yaml."""
from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from datetime import date, time
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[2]
KEY_PATTERN = re.compile(r"^qd_[0-9A-Za-z]{32}$")


def _hhmm(value: str) -> time:
    h, m = value.split(":")
    return time(int(h), int(m))


@dataclass(frozen=True)
class Settings:
    api_key: str
    base_url: str
    budget_per_min: int
    reserve_requests: int
    discord_health_webhook: str | None
    data_dir: Path
    log_level: str
    indices: list[str]
    stocks: list[str]
    timezone: str
    collect_start: time
    collect_end: time
    regular_open: time
    regular_close: time
    live: dict[str, Any]
    backfill: dict[str, Any]
    holidays: frozenset[date] = field(default_factory=frozenset)
    early_closes: frozenset[date] = field(default_factory=frozenset)

    @property
    def universe(self) -> list[str]:
        return [*self.indices, *self.stocks]

    def is_index(self, ticker: str) -> bool:
        return ticker in self.indices


def load_settings(
    env_file: Path | None = None,
    config_file: Path | None = None,
    require_key: bool = True,
) -> Settings:
    load_dotenv(env_file or REPO_ROOT / ".env", override=False)
    cfg = yaml.safe_load((config_file or REPO_ROOT / "config" / "universe.yaml").read_text())

    api_key = os.getenv("QUANTDATA_API_KEY", "").strip()
    if require_key:
        if not api_key:
            raise SystemExit("QUANTDATA_API_KEY is missing. Copy .env.example to .env and add it.")
        if not KEY_PATTERN.match(api_key):
            raise SystemExit("QUANTDATA_API_KEY does not look like qd_ + 32 alphanumerics.")

    data_dir = Path(os.getenv("DATA_DIR", "./data"))
    if not data_dir.is_absolute():
        data_dir = REPO_ROOT / data_dir

    session = cfg["session"]
    return Settings(
        api_key=api_key,
        base_url=os.getenv("QUANTDATA_BASE_URL", "https://api.quantdata.us").rstrip("/"),
        budget_per_min=int(os.getenv("QD_BUDGET_PER_MIN", "150")),
        reserve_requests=int(os.getenv("QD_RESERVE_REQUESTS", "25")),
        discord_health_webhook=os.getenv("DISCORD_WEBHOOK_HEALTH") or None,
        data_dir=data_dir,
        log_level=os.getenv("LOG_LEVEL", "INFO"),
        indices=list(cfg["indices"]),
        stocks=list(cfg["stocks"]),
        timezone=session["timezone"],
        collect_start=_hhmm(session["collect_start"]),
        collect_end=_hhmm(session["collect_end"]),
        regular_open=_hhmm(session["regular_open"]),
        regular_close=_hhmm(session["regular_close"]),
        live=cfg["live"],
        backfill=cfg["backfill"],
        holidays=frozenset(_as_date(d) for d in cfg.get("holidays", [])),
        early_closes=frozenset(_as_date(d) for d in cfg.get("early_closes", [])),
    )


def _as_date(value: Any) -> date:
    return value if isinstance(value, date) else date.fromisoformat(str(value))
