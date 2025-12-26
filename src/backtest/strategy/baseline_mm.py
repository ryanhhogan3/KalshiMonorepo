from __future__ import annotations

from typing import List, Dict, Optional
from backtest.sim.types import OrderAction, SimConfig
from backtest.replay.book_builder import BookState


def _quantize_price(px: float, tick: float) -> float:
    # round to nearest tick
    return round(px / tick) * tick


def strategy_step(book: BookState, inventory: float, config: SimConfig, working_orders: List, state: Dict[str, Optional[str]]) -> List[OrderAction]:
    """Baseline MM v1: cancel/replace, working orders, and inventory skew.

    Args:
        book: current BookState
        inventory: current inventory
        config: SimConfig with strategy parameters
        working_orders: list of Order objects currently active in broker
        state: mutable dict carried across calls to store working order ids, e.g. {'bid_id':..., 'ask_id':...}

    Returns: list of OrderAction to apply this timestep
    """
    actions: List[OrderAction] = []
    if not book or (book.best_bid is None or book.best_ask is None):
        return actions

    tick = config.tick_size
    spread_ticks = int(round((book.best_ask - book.best_bid) / tick)) if tick > 0 else 0
    if spread_ticks < config.min_spread_ticks:
        # spread too tight; do not quote
        return actions

    # compute skew in ticks based on inventory
    skew_ticks = int(round(config.skew_k * inventory))

    # compute target prices with skew and quantize to tick grid
    if inventory > 0:
        # long: be less aggressive on bid, more aggressive on ask
        bid_px = book.best_bid - skew_ticks * tick
        ask_px = book.best_ask - skew_ticks * tick
    elif inventory < 0:
        # short: be more aggressive on bid, less on ask
        bid_px = book.best_bid + abs(skew_ticks) * tick
        ask_px = book.best_ask + abs(skew_ticks) * tick
    else:
        bid_px = book.best_bid
        ask_px = book.best_ask

    bid_px = _quantize_price(bid_px, tick)
    ask_px = _quantize_price(ask_px, tick)

    size = config.order_size

    # find current working orders by side
    bid_work = None
    ask_work = None
    for o in working_orders:
        if o.side == 'buy':
            bid_work = o
        elif o.side == 'sell':
            ask_work = o

    # decide actions for bid
    bid_id = state.get('bid_id')
    if inventory < config.max_inv:
        # want to quote bid
        if bid_work is None:
            # place new bid
            actions.append(OrderAction(kind='place', side='buy', price=bid_px, size=size))
        else:
            # if price or size differs by >= 1 tick, modify (or cancel+place)
            price_diff_ticks = abs(int(round((bid_work.price - bid_px) / tick))) if tick > 0 else 0
            size_diff = abs(bid_work.size - size)
            if price_diff_ticks >= 1 or size_diff != 0:
                # try modify
                actions.append(OrderAction(kind='modify', side='buy', price=bid_px, size=size, order_id=bid_work.order_id))
    else:
        # inventory at or above max: cancel any bid
        if bid_work is not None:
            actions.append(OrderAction(kind='cancel', side='buy', price=bid_work.price, size=bid_work.size, order_id=bid_work.order_id))

    # decide actions for ask
    ask_id = state.get('ask_id')
    if inventory > -config.max_inv:
        # want to quote ask
        if ask_work is None:
            actions.append(OrderAction(kind='place', side='sell', price=ask_px, size=size))
        else:
            price_diff_ticks = abs(int(round((ask_work.price - ask_px) / tick))) if tick > 0 else 0
            size_diff = abs(ask_work.size - size)
            if price_diff_ticks >= 1 or size_diff != 0:
                actions.append(OrderAction(kind='modify', side='sell', price=ask_px, size=size, order_id=ask_work.order_id))
    else:
        if ask_work is not None:
            actions.append(OrderAction(kind='cancel', side='sell', price=ask_work.price, size=ask_work.size, order_id=ask_work.order_id))

    return actions
