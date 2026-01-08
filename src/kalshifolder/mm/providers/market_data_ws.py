import asyncio
import logging
import time
from typing import Dict, List, Optional
import json

from ..utils.logging import json_msg

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
        self._peeked: set[str] = set()

        # Periodic per-ticker truth probe
        self._truth_probe_every_s = float(os.getenv('MM_WS_TRUTH_PROBE_S', '10'))
        self._last_truth_probe_mono: dict[str, float] = {}

        # Hard gating for implied asks
        self._min_spread = float(os.getenv('MM_WS_MIN_SPREAD', '0.01'))
        self._min_spread_cents = max(1, int(round(self._min_spread * 100.0)))

    def _px_to_cents(self, px) -> int | None:
        """Convert px to integer cents (1..99). Accepts 0.xx floats or integer cents."""
        if px is None:
            return None
        try:
            v = float(px)
        except Exception:
            return None

        # If it looks like cents already (>= 1), treat as cents.
        if v >= 1.0:
            c = int(round(v))
        else:
            # dollars 0.xx -> cents
            c = int(round(v * 100.0))

        if c < 1 or c > 99:
            return None
        return c

    def _spread_cents(self, bb_px, ba_px) -> int | None:
        bb = self._px_to_cents(bb_px)
        ba = self._px_to_cents(ba_px)
        if bb is None or ba is None:
            return None
        return ba - bb

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

    def _has_nonzero_levels(self, container) -> bool:
        if container is None:
            return False
        if isinstance(container, dict):
            for _p, s in container.items():
                try:
                    if int(s) > 0:
                        return True
                except Exception:
                    continue
            return False
        if isinstance(container, list):
            for row in container:
                if isinstance(row, dict):
                    try:
                        if int(row.get("size") or row.get("count") or 0) > 0:
                            return True
                    except Exception:
                        continue
                elif isinstance(row, (list, tuple)) and len(row) >= 2:
                    try:
                        if int(row[1]) > 0:
                            return True
                    except Exception:
                        continue
            return False
        return False

    def _maybe_truth_probe(self, *, ticker: str, context: dict) -> None:
        if not self._ws_debug:
            return
        now = time.monotonic()
        last = self._last_truth_probe_mono.get(ticker, 0.0)
        if (now - last) < self._truth_probe_every_s:
            return
        self._last_truth_probe_mono[ticker] = now
        try:
            log.info(json_msg({"event": "ws_truth_probe", "ticker": ticker, **context}))
        except Exception:
            pass

    def _log_ws_drop_update(self, *, ticker: str, reason: str, context: dict | None = None) -> None:
        ctx = context or {}
        try:
            log.error(
                json_msg(
                    {
                        "event": "ws_drop_update",
                        "ticker": ticker,
                        "reason": reason,
                        # keep it bounded so logs don’t explode
                        "context": ctx,
                    }
                )
            )
        except Exception:
            pass

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

        # One-time raw message peek per ticker (debug only). This is bounded.
        try:
            if self._ws_debug and ticker and ticker not in self._peeked:
                self._peeked.add(ticker)
                log.info(
                    json_msg(
                        {
                            "event": "ws_raw_peek",
                            "ticker": ticker,
                            "keys": list(msg.keys())[:60],
                            "type": msg.get("type"),
                            "market_ticker": msg.get("market_ticker")
                            or msg.get("ticker")
                            or msg.get("market"),
                            "snippet": str(msg)[:600],
                        }
                    )
                )
        except Exception:
            pass

        def _norm_px(px):
            if px is None:
                return None
            try:
                v = float(px)
            except Exception:
                return None
            # If we ever see > 1, assume cents and convert to dollars
            if v > 1.5:
                v = v / 100.0
            # clip into Kalshi range
            if v <= 0:
                return None
            if v >= 1:
                v = 0.99
            # quantize to 1-cent tick
            return round(v + 1e-9, 2)

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

        # Presence booleans (raw)
        yes_bids_container = yes_levels.get("bids") if isinstance(yes_levels, dict) else None
        yes_asks_container = yes_levels.get("asks") if isinstance(yes_levels, dict) else None
        no_bids_container = no_levels.get("bids") if isinstance(no_levels, dict) else None
        no_asks_container = no_levels.get("asks") if isinstance(no_levels, dict) else None

        has_yes_bids = self._has_nonzero_levels(yes_bids_container)
        has_yes_asks = self._has_nonzero_levels(yes_asks_container)
        has_no_bids = self._has_nonzero_levels(no_bids_container)
        has_no_asks = self._has_nonzero_levels(no_asks_container)

        # Compute direct BBO first (no derivation)
        yes_bb_direct, yes_ba_direct, yes_bb_sz_direct, yes_ba_sz_direct = self._best_bid_ask(yes_levels)
        no_bb_direct, no_ba_direct, no_bb_sz_direct, no_ba_sz_direct = self._best_bid_ask(no_levels)

        yes_bb_direct = _norm_px(yes_bb_direct)
        yes_ba_direct = _norm_px(yes_ba_direct)
        no_bb_direct = _norm_px(no_bb_direct)
        no_ba_direct = _norm_px(no_ba_direct)

        # Compute implied asks only if asks are missing/empty
        yes_ba_implied = None
        yes_ba_sz_implied = 0
        if yes_ba_direct is None:
            if no_bb_direct is not None:
                yes_ba_implied = round(1.0 - float(no_bb_direct), 2)
                yes_ba_sz_implied = int(no_bb_sz_direct or 0)

        no_ba_implied = None
        no_ba_sz_implied = 0
        if no_ba_direct is None:
            if yes_bb_direct is not None:
                no_ba_implied = round(1.0 - float(yes_bb_direct), 2)
                no_ba_sz_implied = int(yes_bb_sz_direct or 0)

        yes_ba_implied = _norm_px(yes_ba_implied)
        no_ba_implied = _norm_px(no_ba_implied)

        # Choose output (direct preferred)
        yes_bb_px = yes_bb_direct
        yes_bb_sz = int(yes_bb_sz_direct or 0)
        yes_ba_is_implied = False
        if yes_ba_direct is not None:
            yes_ba_px = yes_ba_direct
            yes_ba_sz = int(yes_ba_sz_direct or 0)
        else:
            yes_ba_px = yes_ba_implied
            yes_ba_sz = int(yes_ba_sz_implied or 0)
            yes_ba_is_implied = yes_ba_px is not None

        no_bb_px = no_bb_direct
        no_bb_sz = int(no_bb_sz_direct or 0)
        no_ba_is_implied = False
        if no_ba_direct is not None:
            no_ba_px = no_ba_direct
            no_ba_sz = int(no_ba_sz_direct or 0)
        else:
            no_ba_px = no_ba_implied
            no_ba_sz = int(no_ba_sz_implied or 0)
            no_ba_is_implied = no_ba_px is not None

        # Structured truth probe (periodic per ticker when debug is on)
        probe_ctx = {
            "has_yes_bids": has_yes_bids,
            "has_yes_asks": has_yes_asks,
            "has_no_bids": has_no_bids,
            "has_no_asks": has_no_asks,
            "yes_levels_top": self._levels_preview(yes_levels, max_n=3),
            "no_levels_top": self._levels_preview(no_levels, max_n=3),
            "direct": {
                "yes_bb": yes_bb_direct,
                "yes_ba": yes_ba_direct,
                "no_bb": no_bb_direct,
                "no_ba": no_ba_direct,
            },
            "implied": {
                "yes_ba": yes_ba_implied,
                "no_ba": no_ba_implied,
            },
            "chosen": {
                "yes_bb": yes_bb_px,
                "yes_ba": yes_ba_px,
                "no_bb": no_bb_px,
                "no_ba": no_ba_px,
            },
            "flags": {
                "yes_ba_is_implied": bool(yes_ba_is_implied),
                "no_ba_is_implied": bool(no_ba_is_implied),
            },
        }
        self._maybe_truth_probe(ticker=ticker, context=probe_ctx)

        spread_yes_cents = self._spread_cents(yes_bb_px, yes_ba_px)
        spread_no_cents = self._spread_cents(no_bb_px, no_ba_px)

        # Bounded drop context only (no full raw msg payload)
        drop_ctx = probe_ctx | {
            "min_spread": self._min_spread,
            "min_spread_cents": self._min_spread_cents,
            "spread_yes_cents": spread_yes_cents,
            "spread_no_cents": spread_no_cents,
            "msg_keys": list(msg.keys())[:60],
        }

        def _in_range(v):
            return v is not None and 0.0 < float(v) < 1.0

        if yes_bb_px is not None and not _in_range(yes_bb_px):
            self._log_ws_drop_update(ticker=ticker, reason="yes_bb_out_of_range", context=drop_ctx)
            return None
        if yes_ba_px is not None and not _in_range(yes_ba_px):
            self._log_ws_drop_update(ticker=ticker, reason="yes_ba_out_of_range", context=drop_ctx)
            return None
        if no_bb_px is not None and not _in_range(no_bb_px):
            self._log_ws_drop_update(ticker=ticker, reason="no_bb_out_of_range", context=drop_ctx)
            return None
        if no_ba_px is not None and not _in_range(no_ba_px):
            self._log_ws_drop_update(ticker=ticker, reason="no_ba_out_of_range", context=drop_ctx)
            return None

        # Hard block bad output BEFORE publishing
        if yes_bb_px is not None and yes_ba_px is not None and yes_bb_px >= yes_ba_px:
            self._log_ws_drop_update(ticker=ticker, reason="inverted_yes_bbo", context=drop_ctx)
            return None

        if yes_ba_px is not None and yes_bb_px is not None:
            try:
                if float(yes_ba_px) <= 0.011 and float(yes_bb_px) >= 0.20:
                    self._log_ws_drop_update(ticker=ticker, reason="bogus_yes_ask_symptom", context=drop_ctx)
                    return None
            except Exception:
                pass

        if yes_ba_is_implied and no_bb_direct is None:
            self._log_ws_drop_update(ticker=ticker, reason="cannot_imply_yes_ask_no_no_bb", context=drop_ctx)
            return None

        if yes_ba_is_implied and yes_ba_px is not None and yes_bb_px is not None:
            spr = self._spread_cents(yes_bb_px, yes_ba_px)
            if spr is None:
                self._log_ws_drop_update(ticker=ticker, reason="implied_yes_spread_bad_px", context=drop_ctx)
                return None
            if spr < self._min_spread_cents:
                self._log_ws_drop_update(
                    ticker=ticker,
                    reason="implied_yes_spread_too_small",
                    context=drop_ctx | {"spread_cents": spr},
                )
                return None

        # Symmetric safety for NO side
        if no_bb_px is not None and no_ba_px is not None and no_bb_px >= no_ba_px:
            self._log_ws_drop_update(ticker=ticker, reason="inverted_no_bbo", context=drop_ctx)
            return None
        if no_ba_is_implied and yes_bb_direct is None:
            self._log_ws_drop_update(ticker=ticker, reason="cannot_imply_no_ask_no_yes_bb", context=drop_ctx)
            return None
        if no_ba_is_implied and no_ba_px is not None and no_bb_px is not None:
            spr = self._spread_cents(no_bb_px, no_ba_px)
            if spr is None:
                self._log_ws_drop_update(ticker=ticker, reason="implied_no_spread_bad_px", context=drop_ctx)
                return None
            if spr < self._min_spread_cents:
                self._log_ws_drop_update(
                    ticker=ticker,
                    reason="implied_no_spread_too_small",
                    context=drop_ctx | {"spread_cents": spr},
                )
                return None

        return {
            "yes_bb_px": yes_bb_px,
            "yes_bb_sz": int(yes_bb_sz or 0),
            "yes_ba_px": yes_ba_px,
            "yes_ba_sz": int(yes_ba_sz or 0),
            "no_bb_px": no_bb_px,
            "no_bb_sz": int(no_bb_sz or 0),
            "no_ba_px": no_ba_px,
            "no_ba_sz": int(no_ba_sz or 0),
            "yes_ba_is_implied": bool(yes_ba_is_implied),
            "no_ba_is_implied": bool(no_ba_is_implied),
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
                                    log.info(
                                        json_msg(
                                            {
                                                "event": "ws_first_bbo",
                                                "ticker": ticker,
                                                "yes_bb_px": bbo.get("yes_bb_px"),
                                                "yes_ba_px": bbo.get("yes_ba_px"),
                                                "no_bb_px": bbo.get("no_bb_px"),
                                                "no_ba_px": bbo.get("no_ba_px"),
                                                "yes_ba_is_implied": bbo.get("yes_ba_is_implied"),
                                                "no_ba_is_implied": bbo.get("no_ba_is_implied"),
                                            }
                                        )
                                    )
                                except Exception:
                                    log.info(json_msg({"event": "ws_first_bbo", "ticker": ticker}))

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
                                        log.info(
                                            json_msg(
                                                {
                                                    "event": "ws_first_bbo",
                                                    "ticker": ticker,
                                                    "yes_bb_px": bbo.get("yes_bb_px"),
                                                    "yes_ba_px": bbo.get("yes_ba_px"),
                                                    "no_bb_px": bbo.get("no_bb_px"),
                                                    "no_ba_px": bbo.get("no_ba_px"),
                                                    "yes_ba_is_implied": bbo.get("yes_ba_is_implied"),
                                                    "no_ba_is_implied": bbo.get("no_ba_is_implied"),
                                                }
                                            )
                                        )
                                    except Exception:
                                        log.info(json_msg({"event": "ws_first_bbo", "ticker": ticker}))

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
        yes_ba_is_implied = False
        if no_bb is not None:
            yes_ba = round(1.0 - float(no_bb), 2)
            yes_ba_sz = int(no_bid_sz or 0)
            yes_ba_is_implied = True

        no_ba = None
        no_ba_sz = 0
        no_ba_is_implied = False
        if yes_bb is not None:
            no_ba = round(1.0 - float(yes_bb), 2)
            no_ba_sz = int(yes_bid_sz or 0)
            no_ba_is_implied = True

        def _norm_px(px):
            if px is None:
                return None
            try:
                v = float(px)
            except Exception:
                return None
            if v > 1.5:
                v = v / 100.0
            if v <= 0:
                return None
            if v >= 1:
                v = 0.99
            return round(v + 1e-9, 2)

        yes_bb = _norm_px(yes_bb)
        yes_ba = _norm_px(yes_ba)
        no_bb = _norm_px(no_bb)
        no_ba = _norm_px(no_ba)

        ticker = getattr(ob, 'ticker', None) or ""
        drop_ctx = {
            "has_yes_bids": bool(ob.yes.levels),
            "has_yes_asks": False,
            "has_no_bids": bool(ob.no.levels),
            "has_no_asks": False,
            "direct": {
                "yes_bb": yes_bb,
                "yes_ba": None,
                "no_bb": no_bb,
                "no_ba": None,
            },
            "implied": {
                "yes_ba": yes_ba,
                "no_ba": no_ba,
            },
            "chosen": {
                "yes_bb": yes_bb,
                "yes_ba": yes_ba,
                "no_bb": no_bb,
                "no_ba": no_ba,
            },
            "flags": {
                "yes_ba_is_implied": bool(yes_ba_is_implied),
                "no_ba_is_implied": bool(no_ba_is_implied),
            },
            "min_spread": self._min_spread,
            "min_spread_cents": self._min_spread_cents,
            "spread_yes_cents": self._spread_cents(yes_bb, yes_ba),
            "spread_no_cents": self._spread_cents(no_bb, no_ba),
        }

        def _in_range(v):
            return v is not None and 0.0 < float(v) < 1.0

        if yes_bb is not None and not _in_range(yes_bb):
            self._log_ws_drop_update(ticker=ticker, reason="yes_bb_out_of_range_book_fallback", context=drop_ctx)
            return None
        if yes_ba is not None and not _in_range(yes_ba):
            self._log_ws_drop_update(ticker=ticker, reason="yes_ba_out_of_range_book_fallback", context=drop_ctx)
            return None
        if no_bb is not None and not _in_range(no_bb):
            self._log_ws_drop_update(ticker=ticker, reason="no_bb_out_of_range_book_fallback", context=drop_ctx)
            return None
        if no_ba is not None and not _in_range(no_ba):
            self._log_ws_drop_update(ticker=ticker, reason="no_ba_out_of_range_book_fallback", context=drop_ctx)
            return None

        # Drop inverted / bogus / too-tight implied spreads.
        if yes_bb is not None and yes_ba is not None and yes_bb >= yes_ba:
            self._log_ws_drop_update(ticker=ticker, reason="inverted_yes_bbo_book_fallback", context=drop_ctx)
            return None
        if yes_ba is not None and yes_bb is not None:
            try:
                if float(yes_ba) <= 0.011 and float(yes_bb) >= 0.20:
                    self._log_ws_drop_update(ticker=ticker, reason="bogus_yes_ask_symptom_book_fallback", context=drop_ctx)
                    return None
            except Exception:
                pass
        if yes_ba_is_implied and yes_ba is not None and yes_bb is not None:
            spr = self._spread_cents(yes_bb, yes_ba)
            if spr is None:
                self._log_ws_drop_update(
                    ticker=ticker,
                    reason="implied_yes_spread_bad_px_book_fallback",
                    context=drop_ctx,
                )
                return None
            if spr < self._min_spread_cents:
                self._log_ws_drop_update(
                    ticker=ticker,
                    reason="implied_yes_spread_too_small_book_fallback",
                    context=drop_ctx | {"spread_cents": spr},
                )
                return None
        if no_bb is not None and no_ba is not None and no_bb >= no_ba:
            self._log_ws_drop_update(ticker=ticker, reason="inverted_no_bbo_book_fallback", context=drop_ctx)
            return None
        if no_ba_is_implied and no_ba is not None and no_bb is not None:
            spr = self._spread_cents(no_bb, no_ba)
            if spr is None:
                self._log_ws_drop_update(
                    ticker=ticker,
                    reason="implied_no_spread_bad_px_book_fallback",
                    context=drop_ctx,
                )
                return None
            if spr < self._min_spread_cents:
                self._log_ws_drop_update(
                    ticker=ticker,
                    reason="implied_no_spread_too_small_book_fallback",
                    context=drop_ctx | {"spread_cents": spr},
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
            "yes_ba_is_implied": bool(yes_ba_is_implied),
            "no_ba_is_implied": bool(no_ba_is_implied),
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
