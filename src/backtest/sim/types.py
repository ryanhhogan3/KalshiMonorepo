from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Optional


OrderKind = Literal["place", "cancel", "modify"]
Side = Literal["buy", "sell"]


@dataclass
class OrderAction:
    kind: OrderKind
    side: Side
    price: float
    size: float
    order_id: Optional[str] = None


@dataclass
class Order:
    order_id: str
    side: Side
    price: float
    size: float
    status: str
    created_ts_ms: int
    updated_ts_ms: int


@dataclass
class Fill:
    order_id: str
    side: Side
    price: float
    size: float
    ts_ms: int
    reason: str


@dataclass
class SimConfig:
    tick_size: float = 0.01
    max_inv: float = 100.0
    maker_fill_mode: str = "touch_and_cross"
    fee_bps: float = 0.0
    slippage_ticks: int = 0
    seed: int = 0
    # new config params for baseline strategy
    order_size: float = 1.0
    min_spread_ticks: int = 1
    skew_k: float = 0.0  # ticks per unit inventory to skew quotes
