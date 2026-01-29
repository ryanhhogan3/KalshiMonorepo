from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass
class BBO:
    yes_bb_cents: Optional[int]
    yes_ba_cents: Optional[int]
    ingest_ts_ms: Optional[int]


@dataclass
class WorkingOrder:
    client_order_id: str
    exchange_order_id: Optional[str]
    side: str  # 'BID' (buy yes) or 'ASK' (buy no)
    api_side: str  # 'yes' or 'no'
    price_cents: int
    size: int
    placed_ts_ms: int
