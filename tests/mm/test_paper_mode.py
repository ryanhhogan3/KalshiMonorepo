import asyncio
import time
from kalshifolder.mm.engine import Engine
from kalshifolder.mm.config import MMConfig
from kalshifolder.mm.providers.reconciliation import ReconciliationService
from kalshifolder.mm.state.models import WorkingOrder


class FakeExec:
    def __init__(self):
        self.place_calls = []
        self.cancel_calls = []

    def place_order(self, *args, **kwargs):
        self.place_calls.append((args, kwargs))
        return {'status': 'ACK', 'exchange_order_id': 'X', 'latency_ms': 10, 'raw': '{}'}

    def cancel_order(self, *args, **kwargs):
        self.cancel_calls.append((args, kwargs))
        return {'status': 'ACK', 'exchange_order_id': args[0], 'latency_ms': 5, 'raw': '{}'}

    def get_open_orders(self):
        return []

    def get_positions(self):
        return []

    def get_fills(self, since_ts_ms=0):
        return []


def test_paper_mode_simulates_orders(tmp_path):
    cfg = MMConfig(
        markets=['M1'], poll_ms=250, max_level_age_ms=2000, loop_jitter_ms=50, quote_refresh_ms=1000,
        min_reprice_ticks=1, size=1.0, edge_ticks=1, max_pos=5, max_global_pos=25, max_rejects_per_min=10,
        kill_on_stale=1, kill_on_reject_spike=1, ch_url='http://localhost:8123', ch_user='default', ch_db='kalshi',
        kalshi_base='https://api.kalshi.com', kalshi_key_id='', kalshi_private_key_path='',
        trading_enabled=False, price_units='cents', cancel_strays_enabled=False
    )

    eng = Engine(config=cfg)
    # replace execution provider with fake that will record calls
    fake = FakeExec()
    eng.exec = fake

    # replace market data to produce a valid bb/ba
    class FakeMD:
        def get_batch_best_bid_ask(self, markets):
            return {'M1': {'bb_px': 1.0, 'bb_sz': 1, 'ba_px': 1.02, 'ba_sz': 1, 'ts_ms': int(time.time()*1000)}}

    eng.md = FakeMD()

    # run one iteration
    asyncio.run(eng.run_once())

    # in paper mode, exec.place_order should NOT be called
    assert len(fake.place_calls) == 0
    assert len(fake.cancel_calls) == 0

    # but local working orders should exist and be simulated
    mr = eng.store.get_market('M1')
    assert mr.working_bid is not None or mr.working_ask is not None
    if mr.working_bid:
        assert mr.working_bid.exchange_order_id.startswith('SIMULATED:')
        assert mr.working_bid.status == 'ACKED'
