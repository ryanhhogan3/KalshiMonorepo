import os
from dataclasses import dataclass


@dataclass
class MMConfig:
    markets: list
    poll_ms: int
    max_level_age_ms: int
    loop_jitter_ms: int
    quote_refresh_ms: int
    min_reprice_ticks: int
    size: float
    edge_ticks: int
    max_pos: int
    max_global_pos: int
    max_rejects_per_min: int
    kill_on_stale: int
    kill_on_reject_spike: int
    ch_url: str
    ch_user: str
    ch_db: str
    kalshi_base: str
    kalshi_key_id: str
    kalshi_private_key_path: str
    trading_enabled: bool
    price_units: str
    cancel_strays_enabled: bool


def load_config_from_env() -> MMConfig:
    markets = os.getenv('MM_MARKETS', '')
    markets = [m.strip() for m in markets.split(',') if m.strip()]
    return MMConfig(
        markets=markets,
        poll_ms=int(os.getenv('MM_POLL_MS', '250')),
        max_level_age_ms=int(os.getenv('MM_MAX_LEVEL_AGE_MS', '2000')),
        loop_jitter_ms=int(os.getenv('MM_LOOP_JITTER_MS', '50')),
        quote_refresh_ms=int(os.getenv('MM_QUOTE_REFRESH_MS', '1000')),
        min_reprice_ticks=int(os.getenv('MM_MIN_REPRICE_TICKS', '1')),
        size=float(os.getenv('MM_SIZE', '1')),
        edge_ticks=int(os.getenv('MM_EDGE_TICKS', '1')),
        max_pos=int(os.getenv('MM_MAX_POS', '5')),
        max_global_pos=int(os.getenv('MM_MAX_GLOBAL_POS', '25')),
        max_rejects_per_min=int(os.getenv('MM_MAX_REJECTS_PER_MIN', '10')),
        kill_on_stale=int(os.getenv('MM_KILL_ON_STALE', '1')),
        kill_on_reject_spike=int(os.getenv('MM_KILL_ON_REJECT_SPIKE', '1')),
        ch_url=os.getenv('CH_URL', 'http://clickhouse:8123'),
        ch_user=os.getenv('CH_USER', 'default'),
        ch_db=os.getenv('CH_DB', 'kalshi'),
        kalshi_base=os.getenv('KALSHI_API_BASE', 'https://api.kalshi.com'),
        kalshi_key_id=os.getenv('KALSHI_KEY_ID', ''),
        kalshi_private_key_path=os.getenv('KALSHI_PRIVATE_KEY_PATH', ''),
        trading_enabled=bool(int(os.getenv('MM_TRADING_ENABLED', '0'))),
        price_units=os.getenv('MM_PRICE_UNITS', 'cents'),
        cancel_strays_enabled=bool(int(os.getenv('MM_CANCEL_STRAYS_ENABLED', '0'))),
    )
