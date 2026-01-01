from dataclasses import dataclass, field
from typing import Optional, Dict


@dataclass
class WorkingOrder:
    client_order_id: str
    exchange_order_id: Optional[str]
    side: str  # 'BID' or 'ASK'
    price_cents: int
    size: float
    status: str  # PENDING | ACKED | CANCELLED | REJECTED
    placed_ts_ms: int
    remaining_size: float = 0.0
    last_update_ts_ms: Optional[int] = None


@dataclass
class OrderRef:
    market_ticker: str
    internal_side: str  # 'BID' or 'ASK'
    decision_id: Optional[str]
    client_order_id: str


@dataclass
class MarketRuntimeState:
    ticker: str
    last_bb_px: Optional[float] = None
    last_bb_sz: Optional[float] = None
    last_ba_px: Optional[float] = None
    last_ba_sz: Optional[float] = None
    last_ts_ms: Optional[int] = None
    working_bid: Optional[WorkingOrder] = None
    working_ask: Optional[WorkingOrder] = None
    inventory: float = 0.0
    last_quote_ts_ms: Optional[int] = None
    rejects_rolling_counter: int = 0
    # Whether quoting is disabled due to stale data (cleared when fresh data arrives)
    kill_stale: bool = False
    # Whether market data appeared OK on last successful fetch
    md_ok: bool = True
    # Timestamps for last md cycle outcomes
    last_md_error_ts_ms: Optional[int] = None
    last_md_ok_ts_ms: Optional[int] = None
    # Last ingest/exchange timestamps (ms) seen from latest_levels
    last_ingest_ts_ms: Optional[int] = None
    last_exchange_ts_ms: Optional[int] = None
    # Optional human-readable stale reason (e.g. 'age' or other)
    stale_reason: Optional[str] = None


@dataclass
class EngineState:
    instance_id: str
    version: str
    kill_switch: bool = False
    markets: dict = field(default_factory=dict)
    quote_count: int = 0
    action_count: int = 0
    rejects_count: int = 0
    order_by_exchange_id: Dict[str, OrderRef] = field(default_factory=dict)
    order_by_client_id: Dict[str, OrderRef] = field(default_factory=dict)
