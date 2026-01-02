import asyncio
import logging
import time
from typing import Dict, List, Optional

from kalshifolder.websocket.ws_runtime import KalshiWSRuntime
from kalshifolder.websocket.order_book import OrderBook

from .base import BaseMarketDataProvider

log = logging.getLogger(__name__)


class WSMarketDataProvider(BaseMarketDataProvider):
    """Market data provider backed by Kalshi WebSocket runtime.

    Maintains an in-memory BBO snapshot per ticker and exposes
    get_batch_best_bid_ask(markets) for the Engine to consume.
    """

    def __init__(self):
        self.rt: Optional[KalshiWSRuntime] = None
        self._task: Optional[asyncio.Task] = None
        self._books: Dict[str, OrderBook] = {}
        # bbo map: ticker -> dict with yes_bb_px, yes_bb_sz, yes_ba_px, yes_ba_sz, ingest_ts_ms, exchange_ts_ms
        self._bbo: Dict[str, dict] = {}
        self._lock = asyncio.Lock()

    async def start(self) -> None:
        if self.rt and self._task and not self._task.done():
            return
        self.rt = KalshiWSRuntime()
        await self.rt.start()
        self._task = asyncio.create_task(self._consume_loop(), name="ws_provider_consumer")
        log.info("ws_provider_started")

    async def stop(self) -> None:
        try:
            if self._task:
                self._task.cancel()
                try:
                    await self._task
                except asyncio.CancelledError:
                    pass
            if self.rt:
                await self.rt.stop()
        finally:
            self._task = None
            self.rt = None

    async def _consume_loop(self):
        if not self.rt:
            return
        q = self.rt.queue
        while True:
            raw = await q.get()
            try:
                # frames are JSON strings; reuse the streamer parsing expectations
                import json

                frame = json.loads(raw)
            except Exception:
                log.exception("ws_provider_failed_to_parse_frame")
                continue

            typ = frame.get("type")
            try:
                if typ == "orderbook_snapshot":
                    sid = int(frame.get("sid", -1))
                    seq = int(frame.get("seq", -1))
                    msg = frame.get("msg") or {}
                    ticker = (msg.get("market_ticker") or frame.get("market_ticker") or msg.get("ticker") or frame.get("ticker") or "").strip()
                    market_id = (msg.get("market_id") or frame.get("market_id") or "")
                    if not ticker:
                        continue
                    ob = OrderBook(market_id, ticker)
                    ob.apply_snapshot(msg, seq)
                    now_ms = int(time.time() * 1000)
                    async with self._lock:
                        self._books[ticker] = ob
                        # populate bbo from top of book
                        self._bbo[ticker] = self._extract_bbo_from_book(ob, ingest_ms=now_ms)

                elif typ == "orderbook_delta":
                    sid = int(frame.get("sid", -1))
                    seq = int(frame.get("seq", -1))
                    msg = frame.get("msg") or {}
                    ticker = (msg.get("market_ticker") or frame.get("market_ticker") or msg.get("ticker") or frame.get("ticker") or "").strip()
                    if not ticker:
                        continue
                    ob = self._books.get(ticker)
                    if ob is None:
                        # ignore deltas before snapshot
                        continue
                    # apply delta
                    try:
                        abs_size = ob.apply_delta(msg.get("side"), int(msg.get("price")), int(msg.get("delta")), int(seq))
                    except Exception:
                        abs_size = None
                    now_ms = int(time.time() * 1000)
                    async with self._lock:
                        if abs_size is not None:
                            # update bbo for this ticker
                            self._bbo[ticker] = self._extract_bbo_from_book(ob, ingest_ms=now_ms)

            except Exception:
                log.exception("ws_provider_handler_error")

    def _extract_bbo_from_book(self, ob: OrderBook, ingest_ms: int) -> dict:
        # yes side best bid = max price in yes.levels, best ask = min price in yes.levels
        def best_from(levels: dict, pick_max: bool):
            if not levels:
                return (None, 0)
            if pick_max:
                p = max(levels.keys())
            else:
                p = min(levels.keys())
            return (p, levels.get(p, 0))

        yes_bid_pc, yes_bid_sz = best_from(ob.yes.levels, True)
        yes_ask_pc, yes_ask_sz = best_from(ob.yes.levels, False)
        no_bid_pc, no_bid_sz = best_from(ob.no.levels, True)
        no_ask_pc, no_ask_sz = best_from(ob.no.levels, False)

        def pc_to_d(pc):
            return None if pc is None else (pc / 100.0)

        return {
            "yes_bb_px": pc_to_d(yes_bid_pc),
            "yes_bb_sz": int(yes_bid_sz or 0),
            "yes_ba_px": pc_to_d(yes_ask_pc),
            "yes_ba_sz": int(yes_ask_sz or 0),
            "no_bb_px": pc_to_d(no_bid_pc),
            "no_bb_sz": int(no_bid_sz or 0),
            "no_ba_px": pc_to_d(no_ask_pc),
            "no_ba_sz": int(no_ask_sz or 0),
            "ingest_ts_ms": int(ingest_ms),
            "exchange_ts_ms": None,
        }

    def get_batch_best_bid_ask(self, markets: List[str]) -> Dict[str, dict]:
        out: Dict[str, dict] = {}
        now_ms = int(time.time() * 1000)
        # return snapshot for requested markets
        for m in markets:
            v = None
            # prefer the exact ticker
            try:
                v = self._bbo.get(m)
            except Exception:
                v = None
            if v:
                out[m] = v.copy()
            else:
                out[m] = None
        return out

    async def wait_for_initial_bbo(self, markets: List[str], timeout_ms: int = 5000) -> bool:
        """Wait until each market has at least one BBO or timeout.
        Returns True if all markets became ready, False on timeout.
        """
        deadline = time.monotonic() + (timeout_ms / 1000.0)
        missing = set(markets)
        while time.monotonic() < deadline:
            async with self._lock:
                for m in list(missing):
                    if self._bbo.get(m):
                        missing.discard(m)
            if not missing:
                return True
            await asyncio.sleep(0.05)
        return False

    @property
    def last_ws_message_age(self) -> float:
        try:
            return self.rt.last_ws_message_age if self.rt else float("inf")
        except Exception:
            return float("inf")

    @property
    def is_connected(self) -> bool:
        try:
            return self.rt.is_connected if self.rt else False
        except Exception:
            return False
