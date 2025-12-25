from __future__ import annotations

from typing import List
from backtest.sim.types import Order, Fill
from backtest.replay.book_builder import BookState
from dataclasses import dataclass


def _apply_slippage(price: float, ticks: int, tick_size: float, side: str) -> float:
    if ticks == 0:
        return price
    if side == 'buy':
        # buyer pays more with slippage
        return price + ticks * tick_size
    else:
        # seller gets less
        return price - ticks * tick_size


def check_fills(prev_book: BookState, new_book: BookState, open_orders: List[Order], slippage_ticks: int = 0, tick_size: float = 0.01) -> List[Fill]:
    """Return fills detected when transitioning from prev_book to new_book.

    Rules implemented:
    - Immediate cross: buy order.price >= new_book.best_ask -> fill at new_book.best_ask
    - Immediate cross: sell order.price <= new_book.best_bid -> fill at new_book.best_bid
    - Touch heuristic: if order.price == prev_book.best_bid and new_book.best_bid and new_book.best_bid > order.price -> buy filled at order.price
      (symmetrically for sell)

    This is deterministic given prev and new book states.
    """
    fills: List[Fill] = []
    for o in open_orders:
        if o.side == 'buy':
            # cross
            if new_book.best_ask is not None and o.price >= new_book.best_ask:
                px = _apply_slippage(new_book.best_ask, slippage_ticks, tick_size, 'buy')
                fills.append(Fill(order_id=o.order_id, side='buy', price=px, size=o.size, ts_ms=new_book.ts_ms or 0, reason='cross'))
                continue
            # touch heuristic
            if prev_book and prev_book.best_bid is not None and o.price == prev_book.best_bid:
                if new_book.best_bid is not None and new_book.best_bid > o.price:
                    px = _apply_slippage(o.price, slippage_ticks, tick_size, 'buy')
                    fills.append(Fill(order_id=o.order_id, side='buy', price=px, size=o.size, ts_ms=new_book.ts_ms or 0, reason='touch'))
        else:  # sell
            if new_book.best_bid is not None and o.price <= new_book.best_bid:
                px = _apply_slippage(new_book.best_bid, slippage_ticks, tick_size, 'sell')
                fills.append(Fill(order_id=o.order_id, side='sell', price=px, size=o.size, ts_ms=new_book.ts_ms or 0, reason='cross'))
                continue
            if prev_book and prev_book.best_ask is not None and o.price == prev_book.best_ask:
                if new_book.best_ask is not None and new_book.best_ask < o.price:
                    px = _apply_slippage(o.price, slippage_ticks, tick_size, 'sell')
                    fills.append(Fill(order_id=o.order_id, side='sell', price=px, size=o.size, ts_ms=new_book.ts_ms or 0, reason='touch'))

    return fills
