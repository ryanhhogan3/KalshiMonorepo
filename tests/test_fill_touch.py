import sys
from pathlib import Path

# ensure src/ is importable
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from backtest.replay.book_builder import BookState
from backtest.sim.types import Order
from backtest.sim.fill_model import check_fills


def test_buy_touch_fill_when_price_moves_through():
    prev = BookState()
    prev.bids = {99.0: 100}
    prev.asks = {101.0: 100}
    prev._recompute_tob()

    new = BookState()
    new.bids = {100.0: 100}  # best_bid moved up through 99 -> triggers touch
    new.asks = {101.0: 100}
    new._recompute_tob()

    o = Order(order_id='t1', side='buy', price=99.0, size=1.0, status='open', created_ts_ms=0, updated_ts_ms=0)
    fills = check_fills(prev, new, [o], slippage_ticks=0, tick_size=0.01)
    assert len(fills) == 1
    assert fills[0].reason == 'touch'


def test_no_touch_fill_if_no_price_through():
    prev = BookState()
    prev.bids = {99.0: 100}
    prev.asks = {101.0: 100}
    prev._recompute_tob()

    new = BookState()
    new.bids = {99.0: 100}  # no change
    new.asks = {101.0: 100}
    new._recompute_tob()

    o = Order(order_id='t2', side='buy', price=99.0, size=1.0, status='open', created_ts_ms=0, updated_ts_ms=0)
    fills = check_fills(prev, new, [o], slippage_ticks=0, tick_size=0.01)
    assert len(fills) == 0


def test_no_touch_if_not_exact_price():
    prev = BookState()
    prev.bids = {99.0: 100}
    prev.asks = {101.0: 100}
    prev._recompute_tob()

    new = BookState()
    new.bids = {100.0: 100}
    new.asks = {101.0: 100}
    new._recompute_tob()

    # order not at touch (98.0) should not fill when best_bid moves
    o = Order(order_id='t3', side='buy', price=98.0, size=1.0, status='open', created_ts_ms=0, updated_ts_ms=0)
    fills = check_fills(prev, new, [o], slippage_ticks=0, tick_size=0.01)
    assert len(fills) == 0
