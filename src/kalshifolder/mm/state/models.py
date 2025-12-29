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
    kill_stale: bool = False


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
