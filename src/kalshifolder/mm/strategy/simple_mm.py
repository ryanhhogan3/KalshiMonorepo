from dataclasses import dataclass
from typing import Optional
import math


@dataclass
class Quote:
    bid_px: Optional[float]
    bid_sz: float
    ask_px: Optional[float]
    ask_sz: float


def round_to_tick(price: float, tick_size: float) -> float:
    return round(price / tick_size) * tick_size


def compute_quotes(bb: Optional[float], ba: Optional[float], inventory: float, max_pos: float, edge_ticks: int, tick_size: float, size: float) -> Quote:
    if bb is None or ba is None:
        return Quote(None, 0.0, None, 0.0)
    mid = (bb + ba) / 2.0
    base_edge = edge_ticks * tick_size
    skew = (inventory / max_pos) * base_edge if max_pos != 0 else 0.0
    bid = mid - base_edge - max(0.0, skew)
    ask = mid + base_edge + max(0.0, -skew)
    bid = round_to_tick(bid, tick_size)
    ask = round_to_tick(ask, tick_size)
    # reduce size if near limits
    inv_frac = abs(inventory) / max_pos if max_pos else 0.0
    sz = max(1.0, size * (1.0 - inv_frac))
    return Quote(bid_px=bid, bid_sz=sz, ask_px=ask, ask_sz=sz)
