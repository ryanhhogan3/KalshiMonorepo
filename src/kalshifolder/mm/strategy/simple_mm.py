from dataclasses import dataclass
from typing import Optional
import math


@dataclass
class Quote:
    """Two-sided BUY-only quotes (no shorting)"""
    yes_bid_px: Optional[float]     # BUY YES price in dollars
    yes_bid_sz: float               # BUY YES size
    no_bid_px: Optional[float]      # BUY NO price in dollars
    no_bid_sz: float                # BUY NO size
    
    # Deprecated fields (kept for compatibility, computed from yes/no):
    @property
    def bid_px(self):
        return self.yes_bid_px
    
    @property
    def bid_sz(self):
        return self.yes_bid_sz
    
    @property
    def ask_px(self):
        # Ask no longer exists; return inverse of no_bid for reference
        return 100.0 - self.no_bid_px if self.no_bid_px else None
    
    @property
    def ask_sz(self):
        return self.no_bid_sz


def round_to_tick(price: float, tick_size: float) -> float:
    return round(price / tick_size) * tick_size


def compute_quotes(bb: Optional[float], ba: Optional[float], inventory: float, max_pos: float, edge_ticks: int, tick_size: float, size: float) -> Quote:
    """
    Compute two-sided BUY-only quotes.
    
    Returns a Quote with:
    - yes_bid_px: price for BUY YES
    - no_bid_px: price for BUY NO (derived as 100 - yes_bid_px to maintain market consistency)
    
    This avoids shorting and ensures inventory naturally mean-reverts.
    """
    if bb is None or ba is None:
        return Quote(yes_bid_px=None, yes_bid_sz=0.0, no_bid_px=None, no_bid_sz=0.0)
    
    mid = (bb + ba) / 2.0
    base_edge = edge_ticks * tick_size
    
    # Skew YES bid based on inventory
    # If inventory > 0 (holding YES), bid lower to unwind
    # If inventory < 0 (short YES), bid higher to cover
    skew = (inventory / max_pos) * base_edge if max_pos != 0 else 0.0
    yes_bid = mid - base_edge - max(0.0, skew)
    yes_bid = round_to_tick(yes_bid, tick_size)
    
    # NO bid is inverse: if YES bid is 52¢, NO bid should be 48¢
    # This ensures: yes_bid_px + no_bid_px = 1.00 (in dollars) or 100 (in cents)
    no_bid = 1.0 - yes_bid
    no_bid = round_to_tick(no_bid, tick_size)
    
    # reduce size if near limits
    inv_frac = abs(inventory) / max_pos if max_pos else 0.0
    sz = max(1.0, size * (1.0 - inv_frac))
    
    return Quote(yes_bid_px=yes_bid, yes_bid_sz=sz, no_bid_px=no_bid, no_bid_sz=sz)
