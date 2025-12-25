import sys
from pathlib import Path

# ensure src/ is importable
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from backtest.replay.book_builder import BookState
from backtest.sim.types import Order
from backtest.sim.fill_model import check_fills


def test_buy_cross_fill():
    prev = BookState()
    prev.bids = {99.0: 100}
    prev.asks = {101.0: 100}
    prev._recompute_tob()

    new = BookState()
    new.bids = {99.0: 100}
    new.asks = {101.0: 100}
    new._recompute_tob()

    o = Order(order_id='o1', side='buy', price=101.0, size=1.0, status='open', created_ts_ms=0, updated_ts_ms=0)
    fills = check_fills(prev, new, [o], slippage_ticks=0, tick_size=0.01)
    assert len(fills) == 1
    f = fills[0]
    assert f.price == new.best_ask
    assert f.side == 'buy'
    assert f.size == 1.0


def test_sell_cross_fill():
    prev = BookState()
    prev.bids = {99.0: 100}
    prev.asks = {101.0: 100}
    prev._recompute_tob()

    new = BookState()
    new.bids = {100.0: 100}
    new.asks = {101.0: 100}
    new._recompute_tob()

    o = Order(order_id='o2', side='sell', price=99.0, size=2.0, status='open', created_ts_ms=0, updated_ts_ms=0)
    fills = check_fills(prev, new, [o], slippage_ticks=0, tick_size=0.01)
    # here price <= best_bid (99 <= 100) -> fill at best_bid
    assert len(fills) == 1
    f = fills[0]
    assert f.price == new.best_bid
    assert f.side == 'sell'
    assert f.size == 2.0


def test_no_fill_when_side_missing():
    prev = BookState()
    prev.bids = {99.0: 100}
    prev.asks = {}
    prev._recompute_tob()

    new = BookState()
    new.bids = {99.0: 100}
    new.asks = {}
    new._recompute_tob()

    o = Order(order_id='o3', side='buy', price=200.0, size=1.0, status='open', created_ts_ms=0, updated_ts_ms=0)
    fills = check_fills(prev, new, [o], slippage_ticks=0, tick_size=0.01)
    assert len(fills) == 0
