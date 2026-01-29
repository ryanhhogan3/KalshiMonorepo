from __future__ import annotations

from dataclasses import dataclass
import os


@dataclass(frozen=True)
class MMConfig:
    # Kalshi REST
    kalshi_base_url: str

    # Markets
    markets_file: str
    markets_reload_secs: int

    # Engine
    trading_enabled: bool
    poll_ms: int
    quote_refresh_ms: int
    size: int
    spread_cents: int
    min_reprice_cents: int

    # Risk
    max_pos: int
    kill_on_stale: bool
    max_level_age_ms: int

    # SQLite
    sqlite_path: str


def _get_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default


def _get_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in ("1", "true", "t", "yes", "y", "on")


def load_config() -> MMConfig:
    return MMConfig(
        kalshi_base_url=os.getenv("KALSHI_BASE_URL", "https://api.elections.kalshi.com").rstrip("/"),
        markets_file=os.getenv("MM_MARKETS_FILE", "./markets.txt"),
        markets_reload_secs=_get_int("MM_MARKETS_RELOAD_SECS", 5),
        trading_enabled=_get_bool("MM_TRADING_ENABLED", False),
        poll_ms=_get_int("MM_POLL_MS", 500),
        quote_refresh_ms=_get_int("MM_QUOTE_REFRESH_MS", 5000),
        size=_get_int("MM_SIZE", 1),
        spread_cents=_get_int("MM_SPREAD_CENTS", 4),
        min_reprice_cents=_get_int("MM_MIN_REPRICE_CENTS", 2),
        max_pos=_get_int("MM_MAX_POS", 5),
        kill_on_stale=_get_bool("MM_KILL_ON_STALE", True),
        max_level_age_ms=_get_int("MM_MAX_LEVEL_AGE_MS", 5000),
        sqlite_path=os.getenv("MM_SQLITE_PATH", "./mm.sqlite3"),
    )
