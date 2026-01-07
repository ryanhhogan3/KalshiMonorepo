import asyncio
import logging
import time
from typing import Dict, List, Optional
import json

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
        # track which markets we've logged first-BBO for
        self._first_bbo_logged: set = set()
        # debug mode: log first N raw messages
        import os
        self._ws_debug = bool(int(os.getenv('MM_WS_DEBUG', '0')))
        self._ws_debug_limit = int(os.getenv('MM_WS_DEBUG_LIMIT', '20'))
        self._ws_debug_count = 0

    def _best_bid_ask(self, levels: dict) -> tuple[float | None, float | None, int, int]:
        """Compute best bid/ask from a generic levels container.

        levels expected to have:
          - 'bids': {price_cents: size_int} or list[{'price': int, 'size': int}]
          - 'asks': {price_cents: size_int} or list[{'price': int, 'size': int}]
        Returns (bb_px, ba_px, bb_sz, ba_sz) in dollars.
        """
        if not isinstance(levels, dict):
            return (None, None, 0, 0)

        bids = levels.get("bids") or {}
        asks = levels.get("asks") or {}

        def iter_levels(x):
            if isinstance(x, dict):
                for p, s in x.items():
                    try:
                        yield int(p), int(s)
                    except Exception:
                        continue
            elif isinstance(x, list):
                for row in x:
                    if not isinstance(row, dict):
                        continue
                    try:
                        yield int(row.get("price")), int(row.get("size") or row.get("count") or 0)
                    except Exception:
                        continue

        bid_levels = [(p, s) for p, s in iter_levels(bids) if s > 0]
        ask_levels = [(p, s) for p, s in iter_levels(asks) if s > 0]

        bb = None
        ba = None
        bb_sz = 0
        ba_sz = 0

        if bid_levels:
            p, s = max(bid_levels, key=lambda t: t[0])  # best bid = MAX price
            bb, bb_sz = p / 100.0, s

        if ask_levels:
            p, s = min(ask_levels, key=lambda t: t[0])  # best ask = MIN price
            ba, ba_sz = p / 100.0, s

        return bb, ba, int(bb_sz or 0), int(ba_sz or 0)

    def _levels_preview(self, levels: dict, *, max_n: int = 5) -> dict:
        if not isinstance(levels, dict):
            return {"bids": [], "asks": []}

        def to_pairs(x) -> list[tuple[int, int]]:
            pairs: list[tuple[int, int]] = []
            if isinstance(x, dict):
                for p, s in x.items():
                    try:
                        pairs.append((int(p), int(s)))
                    except Exception:
                        continue
            elif isinstance(x, list):
                for row in x:
                    if not isinstance(row, dict):
                        continue
                    try:
                        pairs.append((int(row.get("price")), int(row.get("size") or row.get("count") or 0)))
                    except Exception:
                        continue
            return pairs

        bids = sorted([(p, s) for p, s in to_pairs(levels.get("bids") or {}) if s > 0], key=lambda t: t[0], reverse=True)[:max_n]
        asks = sorted([(p, s) for p, s in to_pairs(levels.get("asks") or {}) if s > 0], key=lambda t: t[0])[:max_n]
        return {"bids": bids, "asks": asks}

    def _extract_bbo_from_msg(self, *, ticker: str, msg: dict, ingest_ms: int) -> dict | None:
        """Extract BBO from a WS message.

        This is intentionally strict: if we detect inverted BBOs we drop the update
        rather than poisoning the engine.
        """
        if not isinstance(msg, dict):
            return None

        # Try to locate YES/NO levels in common formats.
        # Preferred: msg['yes'] and msg['no'] are dicts with {'bids':..., 'asks':...}
        yes_levels = msg.get("yes")
        no_levels = msg.get("no")

        # Alternate: msg contains yes_bids/yes_asks etc.
        if yes_levels is None and (msg.get("yes_bids") is not None or msg.get("yes_asks") is not None):
            yes_levels = {"bids": msg.get("yes_bids") or {}, "asks": msg.get("yes_asks") or {}}
        if no_levels is None and (msg.get("no_bids") is not None or msg.get("no_asks") is not None):
            no_levels = {"bids": msg.get("no_bids") or {}, "asks": msg.get("no_asks") or {}}

        # Legacy in this repo: msg['yes'] and msg['no'] are list pairs [price_cents, size].
        # Treat those as bids-only (asks unknown).
        def pairs_to_levels(pairs) -> dict:
            bids_list: list[dict] = []
            if isinstance(pairs, list):
                for row in pairs:
                    try:
                        p, s = row
                        bids_list.append({"price": int(p), "size": int(s)})
                    except Exception:
                        continue
            return {"bids": bids_list, "asks": []}

        if isinstance(yes_levels, list):
            yes_levels = pairs_to_levels(yes_levels)
        if isinstance(no_levels, list):
            no_levels = pairs_to_levels(no_levels)

        if not isinstance(yes_levels, dict) or not isinstance(no_levels, dict):
            return None

        yes_bb, yes_ba, yes_bb_sz, yes_ba_sz = self._best_bid_ask(yes_levels)
        no_bb, no_ba, no_bb_sz, no_ba_sz = self._best_bid_ask(no_levels)

        # If asks are missing in the WS payload but we have both ladders,
        # we can infer the opposite-side ask via price complement (Kalshi YES/NO shares).
        derived = False
        if yes_ba is None and no_bb is not None:
            yes_ba = round(1.0 - float(no_bb), 2)
            yes_ba_sz = int(no_bb_sz or 0)
            derived = True
        if no_ba is None and yes_bb is not None:
            no_ba = round(1.0 - float(yes_bb), 2)
            no_ba_sz = int(yes_bb_sz or 0)
            derived = True

        # Invariant checks: drop update if inverted.
        if yes_bb is not None and yes_ba is not None and yes_bb >= yes_ba:
            log.error(
                "ws_bbo_inverted",
                extra={
                    "event": "ws_bbo_inverted",
                    "market": ticker,
                    "yes_bb": yes_bb,
                    "yes_ba": yes_ba,
                    "no_bb": no_bb,
                    "no_ba": no_ba,
                    "note": "WS parsing bug likely: ask derived from bids or wrong key",
                },
            )
            # truth probe
            try:
                log.error(
                    "ws_truth_probe",
                    extra={
                        "event": "ws_truth_probe",
                        "market": ticker,
                        "derived": derived,
                        "yes_levels_preview": self._levels_preview(yes_levels),
                        "no_levels_preview": self._levels_preview(no_levels),
                        "computed": {
                            "yes_bb": yes_bb,
                            "yes_ba": yes_ba,
                            "no_bb": no_bb,
                            "no_ba": no_ba,
                        },
                        "raw_levels_snippet": json.dumps(
                            {"yes": msg.get("yes"), "no": msg.get("no"), "keys": list(msg.keys())},
                            default=str,
                        )[:800],
                    },
                )
            except Exception:
                pass
            return None

        # Targeted truth probe when ask is tiny or <= bid (even if engine might drop later)
        try:
            if yes_bb is not None and yes_ba is not None and (yes_ba <= 0.011 or yes_ba <= yes_bb):
                log.error(
                    "ws_truth_probe",
                    extra={
                        "event": "ws_truth_probe",
                        "market": ticker,
                        "derived": derived,
                        "yes_levels_preview": self._levels_preview(yes_levels),
                        "no_levels_preview": self._levels_preview(no_levels),
                        "computed": {
                            "yes_bb": yes_bb,
                            "yes_ba": yes_ba,
                            "no_bb": no_bb,
                            "no_ba": no_ba,
                        },
                        "raw_levels_snippet": json.dumps(
                            {"yes": msg.get("yes"), "no": msg.get("no"), "keys": list(msg.keys())},
                            default=str,
                        )[:800],
                    },
                )
        except Exception:
            pass

        return {
            "yes_bb_px": yes_bb,
            "yes_bb_sz": int(yes_bb_sz or 0),
            "yes_ba_px": yes_ba,
            "yes_ba_sz": int(yes_ba_sz or 0),
            "no_bb_px": no_bb,
            "no_bb_sz": int(no_bb_sz or 0),
            "no_ba_px": no_ba,
            "no_ba_sz": int(no_ba_sz or 0),
            "ingest_ts_ms": int(ingest_ms),
            "exchange_ts_ms": None,
        }

    async def start(self, markets: Optional[List[str]] = None) -> None:
        if self.rt and self._task and not self._task.done():
            return
        self.rt = KalshiWSRuntime()
        await self.rt.start()
        # wait briefly for connect
        for _ in range(50):
            if getattr(self.rt, 'is_connected', False):
                break
            await asyncio.sleep(0.1)

        try:
            log.info("ws_connected", extra={"url": getattr(self.rt, 'ws_url', None), "markets_count": len(markets) if markets else 0})
        except Exception:
            log.info("ws_connected")

        # subscribe if markets provided
        if markets:
            try:
                await self.rt.subscribe_markets(markets)
                try:
                    sample = markets[:3]
                    log.info("ws_subscribed", extra={"markets_sample": sample, "n": len(markets)})
                except Exception:
                    log.info("ws_subscribed")
            except Exception:
                log.exception("ws_subscribe_failed")

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
                frame = json.loads(raw)
                # debug mode: log first N raw frames keys and a small snippet
                try:
                    if self._ws_debug and self._ws_debug_count < self._ws_debug_limit:
                        self._ws_debug_count += 1
                        keys = list(frame.keys()) if isinstance(frame, dict) else []
                        snippet = None
                        try:
                            snippet = frame.get('msg') if isinstance(frame, dict) else None
                        except Exception:
                            snippet = None
                        log.info("ws_raw_msg", extra={"keys": keys, "type": frame.get('type') if isinstance(frame, dict) else None, "snippet": str(snippet)[:200]})
                        if self._ws_debug_count >= self._ws_debug_limit:
                            log.info("ws_debug_logging_complete", extra={"count": self._ws_debug_count})
                except Exception:
                    pass
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
                        # populate bbo from WS message (strict; may drop inverted updates)
                        bbo = self._extract_bbo_from_msg(ticker=ticker, msg=msg, ingest_ms=now_ms)
                        if bbo is not None:
                            self._bbo[ticker] = bbo
                        if ticker not in self._first_bbo_logged:
                            self._first_bbo_logged.add(ticker)
                            try:
                                bb = self._bbo[ticker].get('yes_bb_px')
                                ba = self._bbo[ticker].get('yes_ba_px')
                                log.info('ws_first_bbo', extra={'market': ticker, 'bb': bb, 'ba': ba})
                            except Exception:
                                log.info('ws_first_bbo', extra={'market': ticker})

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
                            # update bbo for this ticker.
                            # Prefer extracting from msg if it includes levels, otherwise fall back to book.
                            bbo = self._extract_bbo_from_msg(ticker=ticker, msg=msg, ingest_ms=now_ms)
                            if bbo is None:
                                bbo = self._extract_bbo_from_book(ob, ingest_ms=now_ms)
                            if bbo is not None:
                                self._bbo[ticker] = bbo
                            if ticker not in self._first_bbo_logged:
                                self._first_bbo_logged.add(ticker)
                                try:
                                    bb = self._bbo[ticker].get('yes_bb_px')
                                    ba = self._bbo[ticker].get('yes_ba_px')
                                    log.info('ws_first_bbo', extra={'market': ticker, 'bb': bb, 'ba': ba})
                                except Exception:
                                    log.info('ws_first_bbo', extra={'market': ticker})

            except Exception:
                log.exception("ws_loop_crash")

    def _extract_bbo_from_book(self, ob: OrderBook, ingest_ms: int) -> dict | None:
        # NOTE: In this repo's OrderBook, we only track a single ladder per side (yes/no).
        # We cannot observe explicit asks from this structure. We treat these ladders as bids-only
        # and infer the opposite-side ask by price complement.
        def best_bid_from(levels: dict):
            if not levels:
                return (None, 0)
            p = max(levels.keys())
            return (p, levels.get(p, 0))

        yes_bid_pc, yes_bid_sz = best_bid_from(ob.yes.levels)
        no_bid_pc, no_bid_sz = best_bid_from(ob.no.levels)

        def pc_to_d(pc):
            return None if pc is None else (pc / 100.0)

        yes_bb = pc_to_d(yes_bid_pc)
        no_bb = pc_to_d(no_bid_pc)

        yes_ba = None
        yes_ba_sz = 0
        if no_bb is not None:
            yes_ba = round(1.0 - float(no_bb), 2)
            yes_ba_sz = int(no_bid_sz or 0)

        no_ba = None
        no_ba_sz = 0
        if yes_bb is not None:
            no_ba = round(1.0 - float(yes_bb), 2)
            no_ba_sz = int(yes_bid_sz or 0)

        # Drop inverted updates rather than poisoning engine.
        if yes_bb is not None and yes_ba is not None and yes_bb >= yes_ba:
            log.error(
                "ws_bbo_inverted",
                extra={
                    "event": "ws_bbo_inverted",
                    "market": getattr(ob, 'ticker', None),
                    "yes_bb": yes_bb,
                    "yes_ba": yes_ba,
                    "no_bb": no_bb,
                    "no_ba": no_ba,
                    "note": "OrderBook has bids-only ladders; complement produced inverted book",
                },
            )
            return None

        return {
            "yes_bb_px": yes_bb,
            "yes_bb_sz": int(yes_bid_sz or 0),
            "yes_ba_px": yes_ba,
            "yes_ba_sz": int(yes_ba_sz or 0),
            "no_bb_px": no_bb,
            "no_bb_sz": int(no_bid_sz or 0),
            "no_ba_px": no_ba,
            "no_ba_sz": int(no_ba_sz or 0),
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

    def is_ready(self, market: str) -> bool:
        try:
            return bool(self._bbo.get(market))
        except Exception:
            return False

    async def wait_ready(self, markets: List[str], timeout_s: float = 10.0) -> bool:
        try:
            return await self.wait_for_initial_bbo(markets, timeout_ms=int(timeout_s * 1000))
        except Exception:
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
