import asyncio
import os
import logging
import time
import csv
import io
from typing import Dict
import json
from .config import load_config_from_env
from .storage.ch_writer import ClickHouseWriter
from .providers.market_data import ClickHouseMarketDataProvider
from .providers.execution import KalshiExecutionProvider
from .providers.reconciliation import ReconciliationService
from .state.models import EngineState, WorkingOrder, OrderRef
from .state.store import EngineStateStore
from .strategy.simple_mm import compute_quotes
from .risk.limits import RiskManager
from .utils.id import uuid4_hex
from .utils.time import now_ms
from .utils.logging import setup_logging, json_msg
from collections import deque
from datetime import datetime, timezone
import requests

logger = logging.getLogger(__name__)

# Ensure a default event loop is available for code/tests that call
# `asyncio.get_event_loop().run_until_complete(...)` in older styles.
try:
    asyncio.get_event_loop()
except RuntimeError:
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    except Exception:
        pass
else:
    # Make get_event_loop resilient in test environments that clear the loop.
    # Replace only if not already patched.
    if not hasattr(asyncio, '_orig_get_event_loop'):
        asyncio._orig_get_event_loop = asyncio.get_event_loop

        def _get_event_loop_resilient():
            try:
                return asyncio._orig_get_event_loop()
            except RuntimeError:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                return loop

        try:
            asyncio.get_event_loop = _get_event_loop_resilient
        except Exception:
            pass


def to_ms(val):
    """Normalize various timestamp formats to integer milliseconds.

    Accepts:
    - int (assume ms if > 1e12 else seconds -> *1000)
    - float (same logic)
    - ISO datetime string parseable by datetime.fromisoformat
    - numeric string

    Returns int ms or None on failure.
    """
    if val is None:
        return None
    try:
        # ints/floats
        if isinstance(val, (int, float)):
            v = int(val)
            if v > 10 ** 12:
                return v
            return int(v * 1000)
        if isinstance(val, str):
            s = val.strip()
            # numeric string?
            if s.isdigit():
                v = int(s)
                if v > 10 ** 12:
                    return v
                return int(v * 1000)
            # try ISO parse
            from datetime import datetime

            try:
                dt = datetime.fromisoformat(s)
                return int(dt.timestamp() * 1000)
            except Exception:
                # last-ditch try to parse common space separated format
                try:
                    dt = datetime.strptime(s, "%Y-%m-%d %H:%M:%S.%f")
                    return int(dt.timestamp() * 1000)
                except Exception:
                    return None
    except Exception:
        return None
    return None


class Engine:
    def __init__(self, config=None):
        self.config = config or load_config_from_env()
        setup_logging()
        self.ch = ClickHouseWriter(
            self.config.ch_url,
            user=self.config.ch_user,
            pwd=self.config.ch_pwd,
            database=self.config.ch_db,
        )
        # Market data source: prefer WS for live quoting unless overridden
        md_source = os.getenv('MM_MD_SOURCE', 'ws').lower()
        if md_source == 'clickhouse':
            self.md = ClickHouseMarketDataProvider(
                self.config.ch_url,
                user=self.config.ch_user,
                pwd=self.config.ch_pwd,
                db=self.config.ch_db,
            )
        else:
            # default: websocket provider (instantiate lazily at run-time)
            self.md = None
            self._md_source = 'ws'
        self.exec = KalshiExecutionProvider(self.config.kalshi_base, key_id=self.config.kalshi_key_id, private_key_path=self.config.kalshi_private_key_path)
        # configure execution provider price units from config
        try:
            self.exec.set_price_units(self.config.price_units)
        except Exception:
            pass
        # pass engine reference to reconciliation service so it can update state and log via engine
        self.recon = ReconciliationService(self, self.exec, ch_writer=self.ch)
        self.state = EngineState(instance_id=uuid4_hex(), version='dev')
        self.store = EngineStateStore(self.state)
        self.risk = RiskManager(params={
            'MM_MAX_POS': self.config.max_pos,
            'MM_MAX_REJECTS_PER_MIN': self.config.max_rejects_per_min,
            'MM_KILL_ON_STALE': self.config.kill_on_stale,
            'MM_KILL_ON_REJECT_SPIKE': self.config.kill_on_reject_spike,
        })
        self._running = False
        self.market_locks: Dict[str, asyncio.Lock] = {}
        self.last_sent: Dict[str, dict] = {}
        self.reject_window_s = int(os.getenv("MM_REJECT_WINDOW_S", "60"))
        self.reject_times = deque()  # store epoch seconds of rejects (global)

    def ch_utc_now_str():
    # ClickHouse DateTime64(3,'UTC') accepts "YYYY-MM-DD HH:MM:SS.mmm"
        return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

    async def ensure_schema(self):
        # ensure schemas exist
        try:
            self.ch.ensure_schema(str((__import__('pathlib').Path(__file__).parent / 'storage' / 'schemas.sql')))
        except Exception:
            logger.exception('ensure_schema failed')

    def _log_decision(self, decision_id: str, market: str, bb, ba, mid, spread, target, inv_before, inv_after_est):
        row = {
            'ts': time.strftime('%Y-%m-%d %H:%M:%S'),
            'decision_id': decision_id,
            'engine_instance_id': self.state.instance_id,
            'engine_version': self.state.version,
            'market_ticker': market,
            'bb_px': bb or None,
            'ba_px': ba or None,
            'mid': mid or None,
            'spread': spread or None,
            'fair': None,
            'target_bid_px': target.bid_px,
            'target_ask_px': target.ask_px,
            'target_bid_sz': target.bid_sz,
            'target_ask_sz': target.ask_sz,
            'inv_before': inv_before,
            'inv_after_est': inv_after_est,
            'reason_codes': [],
            'params_json': json.dumps({}),
        }
        try:
            self.ch.insert('mm_decisions', [row])
        except Exception:
            logger.exception('failed to write decision')

    def _log_action(self, action_id: str, decision_id: str, market: str, client_order_id: str, action_type: str, side: str, api_side: str, price: float, price_cents: int, size: float, replace_of: str = '', request_json: dict = None):
        row = {
            'ts': time.strftime('%Y-%m-%d %H:%M:%S'),
            'action_id': action_id,
            'decision_id': decision_id,
            'engine_instance_id': self.state.instance_id,
            'engine_version': self.state.version,
            'market_ticker': market,
            'client_order_id': client_order_id,
            'action_type': action_type,
            'side': side,
            'api_side': api_side,
            'price': price,
            'price_cents': price_cents,
            'size': size,
            'replace_of_client_order_id': replace_of,
            'request_json': json.dumps(request_json or {}),
        }
        try:
            self.ch.insert('mm_order_actions', [row])
        except Exception:
            logger.exception('failed to write action')

    def _log_response(self, action_id: str, market: str, client_order_id: str, status: str, exchange_order_id: str, reject_reason: str, latency_ms: int, response_json: str):
        row = {
            'ts': time.strftime('%Y-%m-%d %H:%M:%S'),
            'action_id': action_id,
            'engine_instance_id': self.state.instance_id,
            'engine_version': self.state.version,
            'market_ticker': market,
            'client_order_id': client_order_id,
            'status': status,
            'exchange_order_id': exchange_order_id or '',
            'reject_reason': reject_reason or '',
            'latency_ms': int(latency_ms or 0),
            'response_json': response_json or '',
        }
        try:
            self.ch.insert('mm_order_responses', [row])
        except Exception:
            logger.exception('failed to write response')

    async def reconcile_loop(self):
        last_fills_ts = 0
        while self._running:
            try:
                fills = self.recon.fetch_and_apply_fills()
                if fills:
                    last_fills_ts = max([f.get('ts', last_fills_ts) for f in fills])
                self.recon.reconcile_positions()
            except Exception:
                logger.exception('recon loop failed')
            await asyncio.sleep(10)

    async def run_once(self):
        markets = self.config.markets
        tick_size = 0.01
        try:
            batch = self.md.get_batch_best_bid_ask(markets)
        except Exception:
            # on batch failure: don't latch stale permanently; mark md as unavailable for this cycle
            logger.exception('market data batch failed; skipping quotes this cycle')
            now = now_ms()
            for m in markets:
                mr = self.store.get_market(m)
                mr.last_md_error_ts_ms = now
                mr.md_ok = False
            return

        now = now_ms()
        for m in markets:
            mr = self.store.get_market(m)
            md = batch.get(m)
            if int(now/1000) != int((getattr(mr, "_last_wo_log_ms", 0))/1000):
                mr._last_wo_log_ms = now
                logger.info(json_msg({"event":"md_row","market":m,"md":md}))
            # normalize ingest timestamp from the batch (try several keys)
            new_ingest_ms = None
            if md:
                new_ingest_ms = to_ms(md.get('ingest_ts_ms') or md.get('ingest_ts') or md.get('ts_ms') or md.get('ts'))

            if md is None:
                # missing row in a successful batch: mark md as unavailable for this cycle
                logger.info(json_msg({"event":"skip_no_book","market":m}))
                mr.md_ok = False
                mr.last_md_error_ts_ms = None
                mr.stale_reason = 'missing_row'
                # do not overwrite last_ingest_ts_ms
            else:
                # if we could parse a new ingest timestamp, update last_ingest_ts_ms
                if new_ingest_ms is not None:
                    mr.last_ingest_ts_ms = new_ingest_ms
                    mr.md_ok = True
                    mr.last_md_ok_ts_ms = now
                    mr.stale_reason = None
                else:
                    # md present but no usable timestamp
                    mr.md_ok = False

            # single age-based staleness check (compare to previous state and log only on change)
            prev_kill = bool(mr.kill_stale)
            age_ms = None
            if mr.last_ingest_ts_ms is not None:
                age_ms = now - mr.last_ingest_ts_ms
            # Decide staleness
            if mr.last_ingest_ts_ms is not None and age_ms <= self.config.max_level_age_ms:
                mr.kill_stale = False
                # only clear stale_reason when we actually received fresh md this cycle
                if mr.md_ok:
                    mr.stale_reason = None
            else:
                mr.kill_stale = True
                if mr.stale_reason is None:
                    mr.stale_reason = 'age'
            if prev_kill != mr.kill_stale:
                logger.info(json_msg({
                    'event': 'stale_market_age',
                    'market': m,
                    'md_ok': mr.md_ok,
                    'stale_reason': mr.stale_reason,
                    'latest_table': getattr(self.config, 'latest_table', None),
                    'now_ms': now,
                    'last_ingest_ts_ms': mr.last_ingest_ts_ms,
                    'age_ms': age_ms,
                    'max_age_ms': self.config.max_level_age_ms,
                }))

            if not md:
                logger.info(json_msg({"event": "skip_no_md_row", "market": m, "stale_reason": mr.stale_reason}))
                continue

            # ---------- normalize/ingest raw book ----------
            yes_bb_px = md.get("yes_bb_px")
            yes_bb_sz = md.get("yes_bb_sz") or 0
            yes_ba_px = md.get("yes_ba_px")
            yes_ba_sz = md.get("yes_ba_sz") or 0

            no_bb_px = md.get("no_bb_px")
            no_bb_sz = md.get("no_bb_sz") or 0
            no_ba_px = md.get("no_ba_px")
            no_ba_sz = md.get("no_ba_sz") or 0

            # Fallback to legacy keys if present
            if yes_bb_px is None:
                yes_bb_px = md.get("bb_px")
            if yes_bb_sz == 0:
                yes_bb_sz = md.get("bb_sz") or 0
            if yes_ba_px is None:
                yes_ba_px = md.get("ba_px")
            if yes_ba_sz == 0:
                yes_ba_sz = md.get("ba_sz") or 0

            # ---------- normalize prices ----------
            def _round_px(x):
                try:
                    return round(float(x), 2)
                except Exception:
                    return None

            # ---------- write normalized state to mr ----------
            mr.last_bb_px = _round_px(yes_bb_px)
            mr.last_bb_sz = int(yes_bb_sz or 0)
            mr.last_ba_px = _round_px(yes_ba_px)
            mr.last_ba_sz = int(yes_ba_sz or 0)

            mr.no_bb_px = _round_px(no_bb_px)
            mr.no_bb_sz = int(no_bb_sz or 0)
            mr.no_ba_px = _round_px(no_ba_px)
            mr.no_ba_sz = int(no_ba_sz or 0)

            # ensure lock exists
            if m not in self.market_locks:
                self.market_locks[m] = asyncio.Lock()
            last = self.last_sent.setdefault(m, {"bid_px": None, "ask_px": None, "ts_ms": 0})

            bb = mr.last_bb_px
            ba = mr.last_ba_px  # may be garbage from WS

            if bb is None:
                logger.info(json_msg({"event": "skip_no_yes_bb", "market": m}))
                continue

            # basic sanity on bid only
            try:
                bb_val = float(bb)
                if not (0.0 <= bb_val <= 1.0):
                    logger.error(json_msg({"event": "skip_bad_yes_bb_range", "market": m, "bb": bb}))
                    continue
            except (ValueError, TypeError):
                logger.error(json_msg({"event": "skip_bad_yes_bb_type", "market": m, "bb": bb}))
                continue

            # Detect suspect WS ask mapping (ask at 0.01 but bid is large)
            if ba is not None:
                try:
                    ba_val = float(ba)
                    bb_val = float(bb)
                    if ba_val <= 0.011 and bb_val >= 0.20:
                        logger.error(json_msg({"event": "suspect_ws_ask_mapping", "market": m, "yes_bb": bb, "yes_ba": ba, "raw_md": md}))
                except (ValueError, TypeError):
                    pass

            # wo_state: rate-limit to 1/sec per market
            now = now_ms()
            if int(now / 1000) != int(getattr(mr, "_last_wo_log_ms", 0) / 1000):
                mr._last_wo_log_ms = now
                logger.info(json_msg({
                    "event": "wo_state",
                    "market": m,
                    "has_bid": bool(mr.working_bid),
                    "has_ask": bool(mr.working_ask),
                    "bid_client": (mr.working_bid.client_order_id if mr.working_bid else None),
                    "ask_client": (mr.working_ask.client_order_id if mr.working_ask else None),
                    "bid_px_cents": (mr.working_bid.price_cents if mr.working_bid else None),
                    "ask_px_cents": (mr.working_ask.price_cents if mr.working_ask else None),
                    "md_yes_bb": bb,
                    "md_yes_ba": ba,
                    "md_no_bb": mr.no_bb_px,
                    "md_no_ba": mr.no_ba_px,
                }))

            # BID-ONLY quoting mode (safe until WS ask mapping is fixed)
            # Uses yes_bb_px as reference, quotes on both sides but derives ask from bid
            edge_ticks = int(self.config.edge_ticks)
            tick_size = 0.01

            bid_px = max(0.01, min(0.99, float(bb) - edge_ticks * tick_size))
            ask_px = max(0.01, min(0.99, float(bb) + edge_ticks * tick_size))

            # enforce non-crossing by construction
            if bid_px >= ask_px:
                ask_px = min(0.99, bid_px + tick_size)

            # build a target object compatible with existing downstream code
            class _Target:
                def __init__(self, bid_px, ask_px, bid_sz, ask_sz):
                    self.bid_px = bid_px
                    self.ask_px = ask_px
                    self.bid_sz = bid_sz
                    self.ask_sz = ask_sz

            target = _Target(bid_px=bid_px, ask_px=ask_px, bid_sz=float(self.config.size), ask_sz=float(self.config.size))
            allowed, reason = self.risk.check_market(mr)

            logger.info(json_msg({
                "event": "quote_eval",
                "market": m,
                "mode": "bid_only",
                "trading_enabled": bool(self.config.trading_enabled),
                "md_ok": bool(mr.md_ok),
                "kill_stale": bool(mr.kill_stale),
                "risk_allowed": bool(allowed),
                "risk_reason": reason,
                "bb": bb,
                "ba": ba,
                "target_bid_px": target.bid_px,
                "target_ask_px": target.ask_px,
                "target_bid_sz": target.bid_sz,
                "target_ask_sz": target.ask_sz,
            }))

            decision_id = uuid4_hex()
            # For bid-only mode, compute mid from bid only to avoid bad ask
            mid = float(bb)
            spread = float(target.ask_px) - float(target.bid_px)
            self._log_decision(decision_id, m, float(bb), float(target.ask_px), mid, spread, target, mr.inventory, mr.inventory)

            if not allowed:
                logger.info(json_msg({"event": "risk_block", "market": m, "reason": reason}))
                continue

            async with self.market_locks[m]:
                # cancellation timeout (ms)
                cancel_timeout_ms = 1000

                # per-market/place defaults and cooldowns
                now_cycle_ms = now_ms()
                if not hasattr(mr, "cooldown_until_ms"):
                    mr.cooldown_until_ms = 0
                if not hasattr(mr, "last_place_bid_ms"):
                    mr.last_place_bid_ms = 0
                if not hasattr(mr, "last_place_ask_ms"):
                    mr.last_place_ask_ms = 0

                # Global market cooldown after rejects
                if now_cycle_ms < mr.cooldown_until_ms:
                    logger.info(json_msg({"event": "cooldown_skip", "market": m, "until_ms": mr.cooldown_until_ms, "now_ms": now_cycle_ms}))
                    continue

                # Minimum replace interval per market (throttle on 250ms poll)
                min_replace_ms = int(os.getenv("MM_MIN_REPLACE_MS", "2000"))
                now = now_ms()

                # Helper to create deterministic client_order_id
                # Keep it short (32 chars max) to avoid Kalshi API client_order_id constraints.
                # Metadata (instance, market, side, timestamp) stored in state/logs instead.
                def make_client_id(side_char: str) -> str:
                    # Format: B_<30-char-hex> or A_<30-char-hex> = 32 chars total
                    return f"{side_char}_{uuid4_hex()[:30]}"

                # BID side replace semantics
                if target.bid_px is not None:
                    # Check minimum replace interval
                    last_bid = getattr(mr, "last_place_bid_ms", 0) or 0
                    if now - last_bid < min_replace_ms:
                        logger.info(json_msg({"event": "throttle_bid", "market": m, "dt_ms": now - last_bid, "min_ms": min_replace_ms}))
                    else:
                        wo = mr.working_bid
                        new_price_cents = int(round(target.bid_px * 100))
                        need_replace = False
                        if wo is None:
                            need_replace = True
                        else:
                            # Check if price/size changed
                            if wo.price_cents != new_price_cents or int(wo.size) != int(target.bid_sz):
                                need_replace = True
                            else:
                                # Same price and size: no-op, don't cancel/replace
                                logger.info(json_msg({"event": "skip_same_quote", "market": m, "side": "BID"}))
                                need_replace = False

                    if need_replace:
                        # if existing working order, cancel it first
                        if wo is not None:
                            action_id = uuid4_hex()
                            cancel_client = wo.client_order_id
                            api_side = 'yes'
                            self._log_action(action_id, decision_id, m, cancel_client, 'CANCEL', 'BID', api_side, wo.price_cents / 100.0, wo.price_cents, wo.size, replace_of='')
                            if not self.config.trading_enabled:
                                resp = {'status': 'SIMULATED', 'latency_ms': 0, 'raw': '{"simulated": true}', 'exchange_order_id': ''}
                            else:
                                cancel_id = wo.exchange_order_id or wo.client_order_id
                                resp = await asyncio.to_thread(self.exec.cancel_order, cancel_id)
                            status = resp.get('status', 'ERROR')
                            exch_id = resp.get('exchange_order_id')
                            # parse reject reason for cancel
                            cancel_reject_reason = ""
                            try:
                                raw = resp.get('raw') or ''
                                j = json.loads(raw) if raw and raw.lstrip().startswith('{') else None
                                if isinstance(j, dict):
                                    err = j.get('error') or {}
                                    cancel_reject_reason = err.get('code') or err.get('message') or err.get('details') or ''
                                    if err.get('details'):
                                        cancel_reject_reason = f"{cancel_reject_reason} | {err.get('details')}"
                            except Exception:
                                cancel_reject_reason = ""

                            if not self.config.trading_enabled:
                                self._log_response(action_id, m, cancel_client, 'SIMULATED', '', '', 0, json.dumps({'simulated': True}))
                            else:
                                self._log_response(action_id, m, cancel_client, status, exch_id, cancel_reject_reason, resp.get('latency_ms', 0), resp.get('raw', ''))

                            if status == 'ACK' or (not self.config.trading_enabled and status == 'SIMULATED'):
                                wo.status = 'CANCELLED'
                                # remove from registries
                                try:
                                    if getattr(wo, 'exchange_order_id', None):
                                        self.state.order_by_exchange_id.pop(str(wo.exchange_order_id), None)
                                    self.state.order_by_client_id.pop(getattr(wo, 'client_order_id', ''), None)
                                except Exception:
                                    logger.exception('failed to cleanup registry on cancel')
                                mr.working_bid = None
                            else:
                                # mark pending cancel and continue after timeout
                                wo.status = 'PENDING_CANCEL'
                                # best-effort wait
                                await asyncio.sleep(cancel_timeout_ms / 1000.0)
                                # after wait, clear local state to avoid blocking
                                mr.working_bid = None

                        # place new order
                        action_id = uuid4_hex()
                        client_order_id = make_client_id('B')
                        api_side = 'yes'
                        self._log_action(action_id, decision_id, m, client_order_id, 'PLACE', 'BID', api_side, target.bid_px, new_price_cents, target.bid_sz, replace_of=(wo.client_order_id if wo else ''))
                        if not self.config.trading_enabled:
                            # simulate place
                            resp = {'status': 'SIMULATED', 'latency_ms': 0, 'raw': '{"simulated": true}', 'exchange_order_id': ''}
                        else:
                            resp = await asyncio.to_thread(self.exec.place_order, m, api_side, new_price_cents, int(target.bid_sz), client_order_id)
                        status = resp.get('status', 'ERROR')
                        exch_id = resp.get('exchange_order_id')
                        # parse reject reason for place
                        reject_reason = ""
                        try:
                            raw = resp.get('raw') or ''
                            j = json.loads(raw) if raw and raw.lstrip().startswith('{') else None
                            if isinstance(j, dict):
                                err = j.get('error') or {}
                                reject_reason = err.get('code') or err.get('message') or err.get('details') or ''
                                if err.get('details'):
                                    reject_reason = f"{reject_reason} | {err.get('details')}"
                        except Exception:
                            reject_reason = ""

                        # log response but mark simulated separately
                        if not self.config.trading_enabled:
                            self._log_response(action_id, m, client_order_id, 'SIMULATED', '', '', 0, json.dumps({'simulated': True}))
                        else:
                            self._log_response(action_id, m, client_order_id, status, exch_id, reject_reason, resp.get('latency_ms', 0), resp.get('raw', ''))

                        if status != 'ACK':
                            # 2s cooldown per reject per market (tune)
                            mr.cooldown_until_ms = now_ms() + 2000
                            mr.rejects_rolling_counter += 1
                            logger.error(json_msg({"event":"place_reject", "market": m, "side": "BID", "reason": reject_reason, "cooldown_ms": 2000}))

                            # hard kill if reject spike enabled
                            if self.config.kill_on_reject_spike and mr.rejects_rolling_counter >= self.config.max_rejects_per_min:
                                logger.error(json_msg({"event":"reject_spike_kill", "market": m, "rejects": mr.rejects_rolling_counter}))
                                self._running = False
                            # do NOT attempt the other side in same cycle after a reject
                            continue

                        # update working order (only if ACK)
                        mr.last_place_bid_ms = now
                        wo_new = None
                        if status == 'ACK' or (not self.config.trading_enabled and status == 'SIMULATED'):
                            # for simulated, set exchange id to a simulated token
                            sim_exch = exch_id or (f"SIMULATED:{client_order_id}" if not self.config.trading_enabled else None)
                            wo_new = WorkingOrder(
                                client_order_id=client_order_id,
                                exchange_order_id=sim_exch,
                                side='BID',
                                price_cents=new_price_cents,
                                size=target.bid_sz,
                                status='ACKED',
                                placed_ts_ms=now,
                                remaining_size=target.bid_sz,
                                last_update_ts_ms=now,
                            )
                            mr.working_bid = wo_new
                            logger.info(json_msg({"event":"wo_set","market":m,"side":"BID","client_order_id":client_order_id,"exchange_order_id":sim_exch,"price_cents":new_price_cents,"size":target.bid_sz}))
                            last['bid_px'] = target.bid_px
                            last['ts_ms'] = now
                            # register in engine order registries
                            try:
                                if exch_id:
                                    self.state.order_by_exchange_id[str(exch_id)] = OrderRef(market_ticker=m, internal_side='BID', decision_id=decision_id, client_order_id=client_order_id)
                                self.state.order_by_client_id[client_order_id] = OrderRef(market_ticker=m, internal_side='BID', decision_id=decision_id, client_order_id=client_order_id)
                            except Exception:
                                logger.exception('failed to update order registry')
                        else:
                            # record rejection
                            mr.rejects_rolling_counter += 1

                # ASK side replace semantics
                if target.ask_px is not None:
                    # Check minimum replace interval
                    last_ask = getattr(mr, "last_place_ask_ms", 0) or 0
                    if now - last_ask < min_replace_ms:
                        logger.info(json_msg({"event": "throttle_ask", "market": m, "dt_ms": now - last_ask, "min_ms": min_replace_ms}))
                    else:
                        wo = mr.working_ask
                        new_price_cents = int(round(target.ask_px * 100))
                        need_replace = False
                        if wo is None:
                            need_replace = True
                        else:
                            # Check if price/size changed
                            if wo.price_cents != new_price_cents or int(wo.size) != int(target.ask_sz):
                                need_replace = True
                            else:
                                # Same price and size: no-op, don't cancel/replace
                                logger.info(json_msg({"event": "skip_same_quote", "market": m, "side": "ASK"}))
                                need_replace = False

                        if need_replace:
                            if wo is not None:
                                action_id = uuid4_hex()
                            cancel_client = wo.client_order_id
                            api_side = 'no'
                            self._log_action(action_id, decision_id, m, cancel_client, 'CANCEL', 'ASK', api_side, wo.price_cents / 100.0, wo.price_cents, wo.size, replace_of='')
                            # simulate cancel in paper mode
                            if not self.config.trading_enabled:
                                resp = {'status': 'SIMULATED', 'latency_ms': 0, 'raw': '{"simulated": true}', 'exchange_order_id': ''}
                            else:
                                cancel_id = wo.exchange_order_id or wo.client_order_id
                                resp = await asyncio.to_thread(self.exec.cancel_order, cancel_id)
                            status = resp.get('status', 'ERROR')
                            exch_id = resp.get('exchange_order_id')
                            # parse reject reason for cancel
                            cancel_reject_reason = ""
                            try:
                                raw = resp.get('raw') or ''
                                j = json.loads(raw) if raw and raw.lstrip().startswith('{') else None
                                if isinstance(j, dict):
                                    err = j.get('error') or {}
                                    cancel_reject_reason = err.get('code') or err.get('message') or err.get('details') or ''
                                    if err.get('details'):
                                        cancel_reject_reason = f"{cancel_reject_reason} | {err.get('details')}"
                            except Exception:
                                cancel_reject_reason = ""

                            if not self.config.trading_enabled:
                                self._log_response(action_id, m, cancel_client, 'SIMULATED', '', '', 0, json.dumps({'simulated': True}))
                            else:
                                self._log_response(action_id, m, cancel_client, status, exch_id, cancel_reject_reason, resp.get('latency_ms', 0), resp.get('raw', ''))
                            if status == 'ACK' or (not self.config.trading_enabled and status == 'SIMULATED'):
                                wo.status = 'CANCELLED'
                                try:
                                    if getattr(wo, 'exchange_order_id', None):
                                        self.state.order_by_exchange_id.pop(str(wo.exchange_order_id), None)
                                    self.state.order_by_client_id.pop(getattr(wo, 'client_order_id', ''), None)
                                except Exception:
                                    logger.exception('failed to cleanup registry on cancel')
                                mr.working_ask = None
                            else:
                                wo.status = 'PENDING_CANCEL'
                                await asyncio.sleep(cancel_timeout_ms / 1000.0)
                                mr.working_ask = None

                        action_id = uuid4_hex()
                        client_order_id = make_client_id('A')
                        api_side = 'no'
                        self._log_action(action_id, decision_id, m, client_order_id, 'PLACE', 'ASK', api_side, target.ask_px, new_price_cents, target.ask_sz, replace_of=(wo.client_order_id if wo else ''))
                        if not self.config.trading_enabled:
                            resp = {'status': 'SIMULATED', 'latency_ms': 0, 'raw': '{"simulated": true}', 'exchange_order_id': ''}
                        else:
                            resp = await asyncio.to_thread(self.exec.place_order, m, api_side, new_price_cents, int(target.ask_sz), client_order_id)
                        status = resp.get('status', 'ERROR')
                        exch_id = resp.get('exchange_order_id')
                        # parse reject reason for place
                        reject_reason = ""
                        try:
                            raw = resp.get('raw') or ''
                            j = json.loads(raw) if raw and raw.lstrip().startswith('{') else None
                            if isinstance(j, dict):
                                err = j.get('error') or {}
                                reject_reason = err.get('code') or err.get('message') or err.get('details') or ''
                                if err.get('details'):
                                    reject_reason = f"{reject_reason} | {err.get('details')}"
                        except Exception:
                            reject_reason = ""

                        if not self.config.trading_enabled:
                            self._log_response(action_id, m, client_order_id, 'SIMULATED', '', '', 0, json.dumps({'simulated': True}))
                        else:
                            self._log_response(action_id, m, client_order_id, status, exch_id, reject_reason, resp.get('latency_ms', 0), resp.get('raw', ''))
                        
                        if status != 'ACK':
                            # 2s cooldown per reject per market (tune)
                            mr.cooldown_until_ms = now_ms() + 2000
                            mr.rejects_rolling_counter += 1
                            logger.error(json_msg({"event":"place_reject", "market": m, "side": "ASK", "reason": reject_reason, "cooldown_ms": 2000}))

                            # hard kill if reject spike enabled
                            if self.config.kill_on_reject_spike and mr.rejects_rolling_counter >= self.config.max_rejects_per_min:
                                logger.error(json_msg({"event":"reject_spike_kill", "market": m, "rejects": mr.rejects_rolling_counter}))
                                self._running = False
                            # do NOT attempt next cycle until cooldown expires
                            continue

                        # update working order (only if ACK)
                        mr.last_place_ask_ms = now
                        wo_new = None
                        if status == 'ACK' or (not self.config.trading_enabled and status == 'SIMULATED'):
                            # for simulated, set exchange id to a simulated token
                            sim_exch = exch_id or (f"SIMULATED:{client_order_id}" if not self.config.trading_enabled else None)
                            wo_new = WorkingOrder(
                                client_order_id=client_order_id,
                                exchange_order_id=sim_exch,
                                side='ASK',
                                price_cents=new_price_cents,
                                size=target.ask_sz,
                                status='ACKED',
                                placed_ts_ms=now,
                                remaining_size=target.ask_sz,
                                last_update_ts_ms=now,
                            )
                            mr.working_ask = wo_new
                            logger.info(json_msg({"event":"wo_set","market":m,"side":"ASK","client_order_id":client_order_id,"exchange_order_id":sim_exch,"price_cents":new_price_cents,"size":target.ask_sz}))
                            last['ask_px'] = target.ask_px
                            last['ts_ms'] = now
                            # register in engine order registries
                            try:
                                if exch_id:
                                    self.state.order_by_exchange_id[str(exch_id)] = OrderRef(market_ticker=m, internal_side='ASK', decision_id=decision_id, client_order_id=client_order_id)
                                self.state.order_by_client_id[client_order_id] = OrderRef(market_ticker=m, internal_side='ASK', decision_id=decision_id, client_order_id=client_order_id)
                            except Exception:
                                logger.exception('failed to update order registry')
                        else:
                            mr.rejects_rolling_counter += 1

    
    def _record_reject_and_maybe_kill(self):
        if not self.config.kill_on_reject_spike:
            return
        now_s = time.time()
        self.reject_times.append(now_s)
        # prune old
        cutoff = now_s - self.reject_window_s
        while self.reject_times and self.reject_times[0] < cutoff:
            self.reject_times.popleft()

        if len(self.reject_times) > int(self.config.max_rejects_per_min):
            logger.error(json_msg({
                "event": "KILL_SWITCH_REJECT_SPIKE",
                "rejects_in_window": len(self.reject_times),
                "window_s": self.reject_window_s,
                "max_per_min": self.config.max_rejects_per_min,
            }))
            self._running = False


    def extract_reject_reason(raw: str) -> str:
        if not raw:
            return ""
        try:
            j = json.loads(raw)
            err = j.get("error") if isinstance(j, dict) else None
            if isinstance(err, dict):
                code = err.get("code", "")
                details = err.get("details", "") or err.get("message", "")
                # keep it compact for CH
                return f"{code}:{details}"[:500]
            return raw[:500]
        except Exception:
            return raw[:500]

    

    async def run(self):
        self._running = True
        await self.ensure_schema()
        # log latest_levels freshness to detect mismatched streamer writes
        # ---- trading preflight ----
        API_PREFIX = "/trade-api/v2"
        if self.config.trading_enabled:
            try:
                # 1) exchange status
                status_path = f"{API_PREFIX}/exchange/status"
                r = requests.get(
                    self.exec.base_url + status_path,
                    headers=self.exec._signed_headers("GET", status_path, ""),
                    timeout=10,
                )
                ok = (r.status_code == 200)
                logger.info(json_msg({"event": "exchange_status", "ok": ok, "code": r.status_code, "body": r.text[:300]}))
                if not ok:
                    logger.error("Trading preflight failed: exchange status not OK. Disabling trading.")
                    self.config.trading_enabled = False
            except Exception:
                logger.exception("Trading preflight failed: exchange status error. Disabling trading.")
                self.config.trading_enabled = False

        if self.config.trading_enabled:
            try:
                # 2) balance threshold
                min_bal = int(os.getenv("MM_MIN_BALANCE_CENTS", "100"))  # default $1.00
                bal_path = f"{API_PREFIX}/portfolio/balance"
                r = requests.get(
                    self.exec.base_url + bal_path,
                    headers=self.exec._signed_headers("GET", bal_path, ""),
                    timeout=10,
                )
                if r.status_code == 200:
                    j = r.json()
                    bal = int(j.get("balance", 0))
                    logger.info(json_msg({"event": "balance_check", "balance_cents": bal, "min_required_cents": min_bal}))
                    if bal < min_bal:
                        logger.error("Insufficient balance for trading. Disabling trading.")
                        self.config.trading_enabled = False
                else:
                    logger.error("Balance check failed code=%s body=%s", r.status_code, r.text[:300])
                    self.config.trading_enabled = False
            except Exception:
                logger.exception("Balance check failed. Disabling trading.")
                self.config.trading_enabled = False

        try:
            await asyncio.to_thread(self._log_latest_table_freshness)
        except Exception:
            logger.exception('latest table freshness check failed')
        # perform startup reconciliation (cancel lingering engine-tagged orders)
        try:
            await asyncio.to_thread(self.recon.startup_reconcile, True)
        except Exception:
            logger.exception('startup_reconcile failed')

        # run one full reconcile cycle to seed inventory and clear stray orders
        try:
            await asyncio.to_thread(self.recon.reconcile_cycle)
        except Exception:
            logger.exception('initial reconcile_cycle failed')

        # start reconcile loop
        # Start market data provider and wait for initial BBOs where applicable
        try:
            # instantiate lazy WS provider if requested
            if getattr(self, '_md_source', None) == 'ws' and not self.md:
                try:
                    # local import to avoid heavy websocket imports at module import time
                    from .providers.market_data_ws import WSMarketDataProvider

                    self.md = WSMarketDataProvider()
                except Exception:
                    logger.exception('failed_to_instantiate_ws_provider')
            if self.md:
                try:
                    await self.md.start(self.config.markets)
                except TypeError:
                    # older providers may not accept markets param
                    await self.md.start()
            wait_ms = int(os.getenv('MM_MD_WAIT_MS', '5000'))
            if hasattr(self.md, 'wait_ready'):
                ok = await self.md.wait_ready(self.config.markets, timeout_s=(wait_ms / 1000.0))
            elif hasattr(self.md, 'wait_for_initial_bbo'):
                ok = await self.md.wait_for_initial_bbo(self.config.markets, timeout_ms=wait_ms)
                if not ok:
                    logger.warning('md provider did not provide initial BBOs within timeout')
        except Exception:
            logger.exception('failed to start market data provider')

        recon_task = asyncio.create_task(self.reconcile_loop())
        try:
            # WS disconnect kill threshold (seconds)
            ws_kill_s = int(os.getenv('MM_WS_DISCONNECT_KILL_S', '150000'))
            while self._running:
                # Hard-kill if WS provider reports long disconnects
                try:
                    if hasattr(self.md, 'last_ws_message_age'):
                        age = float(getattr(self.md, 'last_ws_message_age'))
                        if age is not None and age > ws_kill_s:
                            logger.error('ws_provider_disconnected_too_long age_s=%s killing_engine', age)
                            self._running = False
                            break
                except Exception:
                    pass

                await self.run_once()
                await asyncio.sleep(self.config.poll_ms / 1000.0)
        except asyncio.CancelledError:
            pass
        finally:
            self._running = False
            recon_task.cancel()
            # ensure provider is stopped
            try:
                if hasattr(self.md, 'stop'):
                    await self.md.stop()
            except Exception:
                logger.exception('failed to stop market data provider')

    def stop(self):
        self._running = False

    def _log_latest_table_freshness(self):
        """Check the configured latest table's max ingest_ts and log a warning if it's stale.
        This is a best-effort check and will not raise on failure.
        """
        try:
            # prefer explicit sink/table if available, else fall back to configured DB + v2 name
            try:
                table = getattr(self.ch, 'table_latest', None)
            except Exception:
                table = None
            if not table:
                # prefer explicit configured latest_table if provided
                try:
                    table = getattr(self.config, 'latest_table', None) or ''
                except Exception:
                    table = ''
                if not table:
                    table = f"{self.config.ch_db}.latest_levels_v2"
                else:
                    # normalize: prefix with DB if no dot present
                    if '.' not in table:
                        table = f"{self.config.ch_db}.{table}"

            sql = f"SELECT max(ingest_ts) FROM {table} FORMAT CSV"
            try:
                txt = self.ch._exec(sql)
            except Exception:
                logger.exception('failed to query latest table ingest_ts')
                return
            if not txt:
                return
            val = txt.strip().splitlines()[-1].strip()
            if not val:
                return
            # parse as ISO or epoch seconds
            try:
                from datetime import datetime
                if '-' in val or 'T' in val:
                    dt = datetime.fromisoformat(val)
                    ingest_ms = int(dt.timestamp() * 1000)
                else:
                    ingest_ms = int(float(val) * 1000)
            except Exception:
                return
            age_ms = now_ms() - ingest_ms
            if age_ms > self.config.max_level_age_ms:
                logger.warning('latest table appears stale: %s age_ms=%s table=%s', val, age_ms, table)
            else:
                logger.info('latest table freshness OK: %s age_ms=%s table=%s', val, age_ms, table)
        except Exception:
            logger.exception('unexpected error checking latest table freshness')
