import asyncio
from kalshifolder.mm.config import MMConfig
from kalshifolder.mm.utils.time import now_ms


def make_cfg():
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


class MDRaisesThenGood:
    def __init__(self):
        self.calls = 0

    def get_batch_best_bid_ask(self, markets):
        self.calls += 1
        if self.calls == 1:
            raise Exception('boom')
        now = now_ms()
        return {markets[0]: {'ingest_ts_ms': now, 'yes_bb_px': 1.0, 'yes_ba_px': 1.02, 'yes_bb_sz': 1, 'yes_ba_sz': 1}}


class MDGoodThenMissing:
    def __init__(self):
        self.calls = 0
        self.first_ts = None

    def get_batch_best_bid_ask(self, markets):
        self.calls += 1
        if self.calls == 1:
            now = now_ms()
            self.first_ts = now
            return {markets[0]: {'ingest_ts_ms': now, 'yes_bb_px': 1.0, 'yes_ba_px': 1.02, 'yes_bb_sz': 1, 'yes_ba_sz': 1}}
        # second call: missing the market
        return {}


class MDStringTS:
    def get_batch_best_bid_ask(self, markets):
        # ISO with microseconds
        return {markets[0]: {'ingest_ts': '2026-01-01 17:26:14.217614', 'yes_bb_px': 1.0, 'yes_ba_px': 1.02, 'yes_bb_sz': 1, 'yes_ba_sz': 1}}


async def run_once_async(engine):
    await engine.run_once()


def test_stale_does_not_latch_on_batch_failure():
    cfg = make_cfg()
    from kalshifolder.mm.engine import Engine
    eng = Engine(config=cfg)
    eng.ch = type('C', (), {'insert': lambda *a, **k: None})()
    eng.md = MDRaisesThenGood()

    # first run: raises
    import asyncio as _asyncio
    _asyncio.get_event_loop().run_until_complete(eng.run_once())
    mr = eng.store.get_market('MKT')
    assert mr.md_ok is False

    # second run: good
    import asyncio as _asyncio
    _asyncio.get_event_loop().run_until_complete(eng.run_once())
    mr = eng.store.get_market('MKT')
    assert mr.md_ok is True
    assert mr.kill_stale is False


def test_missing_market_row_does_not_wipe_last_ingest():
    cfg = make_cfg()
    from kalshifolder.mm.engine import Engine
    eng = Engine(config=cfg)
    eng.ch = type('C', (), {'insert': lambda *a, **k: None})()
    eng.md = MDGoodThenMissing()

    # first run: good
    import asyncio as _asyncio
    _asyncio.get_event_loop().run_until_complete(eng.run_once())
    mr = eng.store.get_market('MKT')
    t0 = mr.last_ingest_ts_ms
    assert t0 is not None

    # second run: missing row -> last_ingest should remain unchanged
    import asyncio as _asyncio
    _asyncio.get_event_loop().run_until_complete(eng.run_once())
    mr = eng.store.get_market('MKT')
    assert mr.last_ingest_ts_ms == t0
    assert mr.md_ok is False
    assert mr.stale_reason == 'missing_row'


def test_ingest_ts_string_parses():
    cfg = make_cfg()
    from kalshifolder.mm.engine import Engine
    eng = Engine(config=cfg)
    eng.ch = type('C', (), {'insert': lambda *a, **k: None})()
    class LocalMDStringTS:
        def get_batch_best_bid_ask(self, markets):
            return {markets[0]: {'ingest_ts': '2026-01-01 17:26:14.217614', 'yes_bb_px': 1.0, 'yes_ba_px': 1.02, 'yes_bb_sz': 1, 'yes_ba_sz': 1}}

    eng.md = LocalMDStringTS()

    import asyncio as _asyncio
    _asyncio.get_event_loop().run_until_complete(eng.run_once())
    mr = eng.store.get_market('MKT')
    assert mr.last_ingest_ts_ms is not None
    # ensure parsed to a plausible epoch ms (year 2026)
    import asyncio
    from kalshifolder.mm.config import MMConfig
    from kalshifolder.mm.engine import Engine, to_ms
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


    class MDRaisesThenGood:
        def __init__(self):
            self.calls = 0

        def get_batch_best_bid_ask(self, markets):
            self.calls += 1
            if self.calls == 1:
                raise Exception('boom')
            now = now_ms()
            return {markets[0]: {'ingest_ts_ms': now, 'yes_bb_px': 1.0, 'yes_ba_px': 1.02, 'yes_bb_sz': 1, 'yes_ba_sz': 1}}


    class MDGoodThenMissing:
        def __init__(self):
            self.calls = 0
            self.first_ts = None

        def get_batch_best_bid_ask(self, markets):
            self.calls += 1
            if self.calls == 1:
                now = now_ms()
                self.first_ts = now
                return {markets[0]: {'ingest_ts_ms': now, 'yes_bb_px': 1.0, 'yes_ba_px': 1.02, 'yes_bb_sz': 1, 'yes_ba_sz': 1}}
            # second call: missing the market
            return {}


    class MDStringTS:
        def get_batch_best_bid_ask(self, markets):
            # ISO with microseconds
            return {markets[0]: {'ingest_ts': '2026-01-01 17:26:14.217614', 'yes_bb_px': 1.0, 'yes_ba_px': 1.02, 'yes_bb_sz': 1, 'yes_ba_sz': 1}}


    async def run_once_async(engine: Engine):
        await engine.run_once()


    def test_stale_does_not_latch_on_batch_failure():
        cfg = make_config()
        eng = Engine(config=cfg)
        eng.ch = type('C', (), {'insert': lambda *a, **k: None})()
        eng.md = MDRaisesThenGood()

        # first run: raises
        asyncio.run(run_once_async(eng))
        mr = eng.store.get_market('MKT')
        assert mr.md_ok is False

        # second run: good
        asyncio.run(run_once_async(eng))
        mr = eng.store.get_market('MKT')
        assert mr.md_ok is True
        assert mr.kill_stale is False


    def test_missing_market_row_does_not_wipe_last_ingest():
        cfg = make_config()
        eng = Engine(config=cfg)
        eng.ch = type('C', (), {'insert': lambda *a, **k: None})()
        eng.md = MDGoodThenMissing()

        # first run: good
        asyncio.run(run_once_async(eng))
        mr = eng.store.get_market('MKT')
        t0 = mr.last_ingest_ts_ms
        assert t0 is not None

        # second run: missing row -> last_ingest should remain unchanged
        asyncio.run(run_once_async(eng))
        mr = eng.store.get_market('MKT')
        assert mr.last_ingest_ts_ms == t0
        assert mr.md_ok is False
        assert mr.stale_reason == 'missing_row'


    def test_ingest_ts_string_parses():
        cfg = make_config()
        eng = Engine(config=cfg)
        eng.ch = type('C', (), {'insert': lambda *a, **k: None})()
        eng.md = MDStringTS()

        asyncio.run(run_once_async(eng))
        mr = eng.store.get_market('MKT')
        assert mr.last_ingest_ts_ms is not None
        # ensure parsed to a plausible epoch ms (year 2026)
        assert mr.last_ingest_ts_ms > 1700000000000
