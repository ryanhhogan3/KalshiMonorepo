"""Lightweight book builder that maintains best bid/ask from snapshot + deltas."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional, Dict, Any, List


@dataclass
class BookState:
    # depth-maintaining representation
    bids: Dict[float, float] = field(default_factory=dict)  # price -> size
    asks: Dict[float, float] = field(default_factory=dict)  # price -> size
    best_bid: Optional[float] = None
    best_ask: Optional[float] = None
    ts_ms: Optional[int] = None

    @classmethod
    def from_snapshot_batch(cls, snap_batch: Dict[str, Any]) -> "BookState":
        bs = cls()
        bs.ts_ms = snap_batch.get('ts_ms')
        rows: List[Dict[str, Any]] = snap_batch.get('rows', [])
        # interpret qty sign convention: qty>0 => bid level; qty<0 => ask level; qty==0 => delete
        for r in rows:
            price = r.get('price')
            qty = r.get('qty', 0)
            if price is None:
                continue
            if qty == 0:
                # ensure removal
                bs.bids.pop(price, None)
                bs.asks.pop(price, None)
            elif qty > 0:
                bs.bids[price] = abs(qty)
            else:
                bs.asks[price] = abs(qty)
        bs._recompute_tob()
        return bs

    def apply_delta(self, row: Dict[str, Any]) -> None:
        # row expected to have price, qty, side(optional)
        price = row.get('price')
        qty = row.get('qty', 0)
        ts = row.get('ts_ms')
        if ts:
            self.ts_ms = ts
        if price is None:
            return
        if qty == 0:
            self.bids.pop(price, None)
            self.asks.pop(price, None)
        elif qty > 0:
            # bid
            self.bids[price] = abs(qty)
        else:
            # ask
            self.asks[price] = abs(qty)
        self._recompute_tob()

    def _recompute_tob(self) -> None:
        # derive top-of-book
        if self.bids:
            self.best_bid = max(self.bids.keys())
        else:
            self.best_bid = None
        if self.asks:
            self.best_ask = min(self.asks.keys())
        else:
            self.best_ask = None
