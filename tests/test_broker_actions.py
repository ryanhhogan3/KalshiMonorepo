import sys
from pathlib import Path

# ensure src/ is importable
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from backtest.sim.broker import Broker
from backtest.sim.types import OrderAction


def test_place_cancel_modify():
    b = Broker()
    act_place = OrderAction(kind='place', side='buy', price=99.0, size=1.0)
    o = b.apply_action(act_place)
    assert len(b.open_orders) == 1
    assert b.n_place == 1

    # modify
    ok = b.modify(o.order_id, new_price=98.5, new_size=2.0)
    assert ok
    assert b.n_modify == 1
    assert b.open_orders[o.order_id].price == 98.5

    # cancel
    ok2 = b.cancel(o.order_id)
    assert ok2
    assert b.n_cancel == 1
    assert o.order_id not in b.open_orders


def test_cancel_unknown_is_noop():
    b = Broker()
    assert not b.cancel('nonexistent')
