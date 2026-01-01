import asyncio
import time
from kalshifolder.mm.config import MMConfig
from kalshifolder.mm.engine import Engine
from kalshifolder.mm.utils.time import now_ms


import asyncio
from kalshifolder.mm.config import MMConfig
from kalshifolder.mm.engine import Engine
from kalshifolder.mm.utils.time import now_ms


def make_config():
    return MMConfig(
        markets=['MKT'],
        poll_ms=100,
        max_level_age_ms=2000,
        loop_jitter_ms=50,
        quote_refresh_ms=1000,
        min_reprice_ticks=1,
        size=1.0,
        edge_ticks=1,
        max_pos=5,
        max_global_pos=25,
        max_rejects_per_min=10,
        kill_on_stale=1,
        kill_on_reject_spike=1,
        ch_url='http://localhost:8123',
        ch_user='default',
        ch_db='kalshi',
        kalshi_base='https://api.kalshi.com',
        kalshi_key_id='',
        kalshi_private_key_path='',
        trading_enabled=False,
        price_units='cents',
        cancel_strays_enabled=False,
        ch_pwd='',
        latest_table='latest_levels_v2',
    )


class DummyMD:
    def __init__(self):
        self.calls = 0

    def get_batch_best_bid_ask(self, markets):
        self.calls += 1
        if self.calls == 1:
            raise Exception('boom')
        now = now_ms()
        return {markets[0]: {'ingest_ts_ms': now, 'yes_bb_px': 1.0, 'yes_ba_px': 1.02, 'yes_bb_sz': 1, 'yes_ba_sz': 1}}


class DummyCH:
    def insert(self, *args, **kwargs):
        return None


async def run_once_async(engine: Engine):
    await engine.run_once()


def test_md_recovery_and_latch():
    cfg = make_config()
    eng = Engine(config=cfg)
    eng.md = DummyMD()
    eng.ch = DummyCH()

    # first run: provider raises -> md_ok False and last_md_error_ts_ms set
    asyncio.run(run_once_async(eng))
    mr = eng.store.get_market('MKT')
    assert mr.md_ok is False
    assert mr.last_md_error_ts_ms is not None

    # second run: provider returns fresh data -> md_ok True and last_ingest_ts_ms updated and kill_stale cleared
    asyncio.run(run_once_async(eng))
    mr = eng.store.get_market('MKT')
    assert mr.md_ok is True
    assert mr.last_ingest_ts_ms is not None
    assert mr.kill_stale is False

    # latch regression: manually set kill_stale True then run once with fresh data
    mr.kill_stale = True
    asyncio.run(run_once_async(eng))
    mr = eng.store.get_market('MKT')
    assert mr.kill_stale is False