import time
import pytest

from kalshifolder.mm.providers.reconciliation import ReconciliationService
from kalshifolder.mm.state.models import WorkingOrder, EngineState
from kalshifolder.mm.state.store import EngineStateStore
from kalshifolder.mm.state.models import OrderRef
from kalshifolder.mm.utils.id import uuid4_hex


class FakeExec:
    def __init__(self, open_orders=None, fills=None, positions=None):
        self._open = open_orders or []
        self._fills = fills or []
        self._positions = positions or []
        self.cancel_calls = []

    def get_open_orders(self):
        return list(self._open)

    def get_fills(self, since_ts_ms=0):
        # return all fills and then clear to simulate one-time retrieval
        fs = list(self._fills)
        self._fills = []
        return fs

    def get_positions(self):
        return list(self._positions)

    def cancel_order(self, target):
        self.cancel_calls.append(target)
        return {'status': 'ACK', 'exchange_order_id': target}


class DummyEngine:
    def __init__(self):
        self.state = EngineState(instance_id='ENG1', version='test')
        self.store = EngineStateStore(self.state)

    def _log_action(self, *args, **kwargs):
        pass

    def _log_response(self, *args, **kwargs):
        pass

    def uuid4_hex(self):
        return uuid4_hex()


def test_stray_exchange_order_cancelled():
    execp = FakeExec(open_orders=[{'market_ticker': 'M1', 'exchange_order_id': 'X1', 'client_order_id': None, 'side': 'yes', 'price_cents': 100, 'remaining_size': 1}])
    eng = DummyEngine()
    recon = ReconciliationService(eng, execp, ch_writer=None)
    recon.reconcile_open_orders()
    assert len(execp.cancel_calls) == 1


def test_local_working_order_missing_clears_local():
    execp = FakeExec(open_orders=[])
    eng = DummyEngine()
    mr = eng.store.get_market('M1')
    wo = WorkingOrder(client_order_id='ENG1:M1:B:1', exchange_order_id='X1', side='BID', price_cents=100, size=1, status='ACKED', placed_ts_ms=int(time.time()*1000), remaining_size=1)
    mr.working_bid = wo
    recon = ReconciliationService(eng, execp, ch_writer=None)
    recon.reconcile_open_orders()
    assert mr.working_bid is None


def test_fill_updates_inventory_and_clears_order():
    fill = {'ts': int(time.time()*1000), 'market_ticker': 'M1', 'exchange_order_id': 'X1', 'client_order_id': None, 'side': 'yes', 'price': 100, 'size': 5, 'raw': {'action': 'BUY'}}
    execp = FakeExec(open_orders=[], fills=[fill])
    eng = DummyEngine()
    mr = eng.store.get_market('M1')
    wo = WorkingOrder(client_order_id='ENG1:M1:B:1', exchange_order_id='X1', side='BID', price_cents=100, size=5, status='ACKED', placed_ts_ms=int(time.time()*1000), remaining_size=5)
    mr.working_bid = wo
    recon = ReconciliationService(eng, execp, ch_writer=None)
    norm = recon.fetch_and_apply_fills()
    # inventory updated by +5 (buy yes)
    assert pytest.approx(mr.inventory, rel=1e-6) == 5.0
    assert mr.working_bid is None


def test_positions_overwrite_local_inventory():
    execp = FakeExec(positions=[{'market_ticker': 'M1', 'position': 3, 'avg_cost': 0}])
    eng = DummyEngine()
    mr = eng.store.get_market('M1')
    mr.inventory = 10
    recon = ReconciliationService(eng, execp, ch_writer=None)
    recon.reconcile_positions()
    assert mr.inventory == 3
