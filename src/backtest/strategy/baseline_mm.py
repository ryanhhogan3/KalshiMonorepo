from __future__ import annotations

from typing import List
from backtest.sim.types import OrderAction, SimConfig
from backtest.replay.book_builder import BookState


def strategy_step(book: BookState, inventory: float, config: SimConfig, working_orders: List) -> List[OrderAction]:
    actions: List[OrderAction] = []
    # if book invalid, do nothing
    if not book or (book.best_bid is None or book.best_ask is None):
        return actions

    # inventory clamp
    if inventory < config.max_inv and inventory > -config.max_inv:
        # quote both sides
        bid_px = book.best_bid
        ask_px = book.best_ask
        # fallback to tick adjustments if we prefer to be off-touch
        if bid_px is None or ask_px is None:
            return actions
        # place buy at bid_px, sell at ask_px
        actions.append(OrderAction(kind='place', side='buy', price=bid_px, size=1.0))
        actions.append(OrderAction(kind='place', side='sell', price=ask_px, size=1.0))
    else:
        # if inventory high positive, avoid bidding
        if inventory >= config.max_inv:
            ask_px = book.best_ask
            actions.append(OrderAction(kind='place', side='sell', price=ask_px, size=1.0))
        elif inventory <= -config.max_inv:
            bid_px = book.best_bid
            actions.append(OrderAction(kind='place', side='buy', price=bid_px, size=1.0))

    return actions
