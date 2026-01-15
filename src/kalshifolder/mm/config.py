import os
from dataclasses import dataclass


@dataclass
class MMConfig:
    markets_file_path: str
    markets_reload_secs: int
    poll_ms: int
    max_level_age_ms: int
    loop_jitter_ms: int
    quote_refresh_ms: int
    min_reprice_ticks: int
    size: float
    edge_ticks: int
    max_pos: int
    max_long_pos: int
    max_short_pos: int
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
    ch_pwd: str = ''
    # optional explicit latest table name (may be 'latest_levels_v2' or 'kalshi.latest_levels_v2')
    latest_table: str = ''


def load_config_from_env() -> MMConfig:
    # Markets MUST come from file only - MarketSelector handles reloading
    return MMConfig(
        markets_file_path=os.getenv('MM_MARKETS_FILE', '/app/config/markets.txt'),
        markets_reload_secs=int(os.getenv('MM_MARKETS_RELOAD_SECS', '5')),
        poll_ms=int(os.getenv('MM_POLL_MS', '250')),
        max_level_age_ms=int(os.getenv('MM_MAX_LEVEL_AGE_MS', '2000')),
        loop_jitter_ms=int(os.getenv('MM_LOOP_JITTER_MS', '50')),
        quote_refresh_ms=int(os.getenv('MM_QUOTE_REFRESH_MS', '5000')),  # increased from 1000ms to 5000ms (5s) to reduce churn
        min_reprice_ticks=int(os.getenv('MM_MIN_REPRICE_TICKS', '2')),  # increased from 1 to 2 ticks (0.02) minimum repricing threshold
        size=float(os.getenv('MM_SIZE', '1')),
        edge_ticks=int(os.getenv('MM_EDGE_TICKS', '1')),
        max_pos=int(os.getenv('MM_MAX_POS', '1')),
        max_long_pos=int(os.getenv('MM_MAX_LONG_POS', os.getenv('MM_MAX_POS', '1'))),
        max_short_pos=int(os.getenv('MM_MAX_SHORT_POS', os.getenv('MM_MAX_POS', '1'))),
        max_global_pos=int(os.getenv('MM_MAX_GLOBAL_POS', '10')),
        max_rejects_per_min=int(os.getenv('MM_MAX_REJECTS_PER_MIN', '10')),
        kill_on_stale=int(os.getenv('MM_KILL_ON_STALE', '1')),
        kill_on_reject_spike=int(os.getenv('MM_KILL_ON_REJECT_SPIKE', '1')),
        ch_url=os.getenv('CH_URL', 'http://clickhouse:8123'),
        ch_user=os.getenv('CH_USER', 'default'),
        ch_pwd=os.getenv('CH_PWD', ''),
        ch_db=os.getenv('CH_DB', 'kalshi'),
        latest_table=os.getenv('CLICKHOUSE_LATEST_TABLE', os.getenv('MM_LATEST_TABLE', '')),
        kalshi_base=os.getenv('KALSHI_API_BASE', 'https://api.kalshi.com'),
        kalshi_key_id=os.getenv('KALSHI_KEY_ID', ''),
        kalshi_private_key_path=os.getenv('KALSHI_PRIVATE_KEY_PATH', ''),
        trading_enabled=bool(int(os.getenv('MM_TRADING_ENABLED', '0'))),
        price_units=os.getenv('MM_PRICE_UNITS', 'cents'),
        cancel_strays_enabled=bool(int(os.getenv('MM_CANCEL_STRAYS_ENABLED', '0'))),
    )
