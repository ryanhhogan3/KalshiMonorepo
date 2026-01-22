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
    min_requote_move_cents: int
    min_requote_age_ms: int
    max_quote_ttl_ms: int
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
    # Optional inventory-based skew configuration
    # skew_per_contract_ticks: how many ticks to skew per contract of inventory
    # max_skew_ticks: hard cap on total skew in ticks (both directions)
    skew_per_contract_ticks: int = 0
    max_skew_ticks: int = 0
    # Flatten-only mode configuration
    # flatten_trigger_pos: position (in contracts) where we stop adding risk
    #   and quote only in the flattening direction for that market.
    # flatten_aggress_ticks: how many ticks off the inside we are willing to
    #   go when flattening (controls how aggressive the exit quotes are).
    flatten_trigger_pos: int = 0
    flatten_aggress_ticks: int = 0
    # Maker-only guard (permanently enabled): ensure every order price sits at
    # least maker_guard_ticks away from the opposing best price so we never
    # cross and pay taker fees. Guard value measured in ticks (cents).
    maker_only_mode: bool = True
    maker_only_guard_ticks: int = 1
    # Reconciliation cadence (seconds) for polling REST positions/fills backup.
    reconcile_interval_sec: int = 3
    # Inventory semantics / gating knobs
    inventory_convention: str = 'BUY_YES_POSITIVE'
    inventory_cap_exit_ratio: float = 0.9
    max_open_exposure: float = 25.0
    # Singleton engine lock configuration
    # When enabled, only one engine instance may hold the lock for a given
    # logical key and therefore trade.
    singleton_lock_enabled: bool = True
    # Optional explicit lock key; if empty, the engine will derive one from
    # other config (e.g. account / DB).
    lock_key: str = ''
    # How long (in seconds) a heartbeat keeps the lock alive.
    lock_ttl_sec: int = 30
    # How often (in seconds) to refresh the heartbeat.
    lock_refresh_sec: int = 10


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
        min_requote_move_cents=int(os.getenv('MM_MIN_REQUOTE_MOVE_CENTS', '2')),
        min_requote_age_ms=int(os.getenv('MM_MIN_REQUOTE_AGE_MS', '20000')),
        max_quote_ttl_ms=int(os.getenv('MM_MAX_QUOTE_TTL_MS', os.getenv('MM_WO_STALE_MS', '60000'))),
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
        trading_enabled=bool(int(os.getenv('MM_TRADING_ENABLED', '1'))),
        price_units=os.getenv('MM_PRICE_UNITS', 'cents'),
        cancel_strays_enabled=bool(int(os.getenv('MM_CANCEL_STRAYS_ENABLED', '0'))),
        skew_per_contract_ticks=int(os.getenv('MM_SKEW_PER_CONTRACT_TICKS', '0')),
        max_skew_ticks=int(os.getenv('MM_MAX_SKEW_TICKS', '0')),
        flatten_trigger_pos=int(os.getenv('MM_FLATTEN_TRIGGER_POS', '5')),
        flatten_aggress_ticks=int(os.getenv('MM_FLATTEN_AGGRESS_TICKS', '5')),
        maker_only_mode=True,
        maker_only_guard_ticks=int(os.getenv('MM_MAKER_GUARD_TICKS', '1')),
        reconcile_interval_sec=int(os.getenv('MM_RECONCILE_INTERVAL_SEC', '3')),
        inventory_convention=os.getenv('MM_INVENTORY_CONVENTION', 'BUY_YES_POSITIVE').upper(),
        inventory_cap_exit_ratio=float(os.getenv('MM_INVENTORY_CAP_EXIT_RATIO', '0.9')),
        max_open_exposure=float(os.getenv('MM_MAX_OPEN_EXPOSURE', os.getenv('MM_MAX_POS', '25'))),
        singleton_lock_enabled=bool(int(os.getenv('MM_SINGLETON_LOCK', '1'))),
        lock_key=os.getenv('MM_LOCK_KEY', ''),
        lock_ttl_sec=int(os.getenv('MM_LOCK_TTL_SEC', '30')),
        lock_refresh_sec=int(os.getenv('MM_LOCK_REFRESH_SEC', '10')),
    )
