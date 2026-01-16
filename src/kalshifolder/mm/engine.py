import asyncio
import os
import logging
import time
import csv
import io
from typing import Dict, Set
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
from .utils.price import complement_100
from .market_selector import MarketSelector
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
        self._last_position_source_log_ts = 0  # throttle position source logging to once per minute
        # Latest reconciled positions snapshot (source of truth for risk & mm_positions)
        self.last_exchange_positions_by_ticker: Dict[str, float] = {}
        
        # Initialize MarketSelector for hot-reload support
        self.market_selector = MarketSelector()
        self._last_active_markets: Set[str] = set()
        reload_secs = int(os.getenv('MM_MARKETS_RELOAD_SECS', '5'))
        self.market_selector = MarketSelector(reload_secs=reload_secs)
        self._last_active_markets: Set[str] = set()

    def _get_exchange_pos_by_ticker(self) -> dict:
        """Fetch current positions from exchange and return as dict keyed by ticker."""
        try:
            positions = self.exec.get_positions()
        except Exception:
            logger.exception("get_positions failed")
            return {}
        out = {}
        for p in positions or []:
            try:
                t = p.get("ticker")
                out[t] = int(p.get("position") or 0)
            except Exception:
                continue
        return out

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

    def _check_and_apply_not_found_circuit_breaker(self, market: str, reject_reason: str, mr) -> bool:
        """
        Circuit breaker for not_found errors: if a market gets >= N not_found rejects
        in a short window, auto-disable it for 1 hour.
        Returns True if market is currently disabled, False otherwise.
        """
        now = now_ms()
        
        # Check if disabled and not yet expired
        if mr.disabled_until_ms is not None and now < mr.disabled_until_ms:
            return True  # Still disabled
        elif mr.disabled_until_ms is not None and now >= mr.disabled_until_ms:
            # Expiration time reached; re-enable
            mr.disabled_until_ms = None
            mr.not_found_count = 0
            mr.not_found_window_start_ms = None
            logger.info(json_msg({
                "event": "not_found_circuit_breaker_reset",
                "market": market,
                "reason": "disable_duration_expired"
            }))
            return False
        
        # Check if this reject is "not_found"
        if reject_reason and 'not_found' in reject_reason.lower():
            # Initialize window if needed
            if mr.not_found_window_start_ms is None:
                mr.not_found_window_start_ms = now
                mr.not_found_count = 1
            else:
                # Check if still in window (60 seconds)
                if now - mr.not_found_window_start_ms <= 60_000:
                    mr.not_found_count += 1
                else:
                    # Window expired; reset
                    mr.not_found_window_start_ms = now
                    mr.not_found_count = 1
            
            # Trigger circuit breaker if count >= N (default 3)
            not_found_threshold = int(os.getenv('MM_NOT_FOUND_THRESHOLD', '3'))
            if mr.not_found_count >= not_found_threshold:
                # Disable for 1 hour
                disable_duration_ms = int(os.getenv('MM_NOT_FOUND_DISABLE_MS', 3600_000))
                mr.disabled_until_ms = now + disable_duration_ms
                
                logger.error(json_msg({
                    "event": "not_found_circuit_breaker_triggered",
                    "market": market,
                    "not_found_count": mr.not_found_count,
                    "window_ms": now - mr.not_found_window_start_ms,
                    "disable_until_ms": mr.disabled_until_ms,
                }))
                
                # Cancel working orders if trading enabled
                if self.config.trading_enabled:
                    try:
                        if mr.working_bid and mr.working_bid.exchange_order_id and not mr.working_bid.exchange_order_id.startswith('SIMULATED:'):
                            self.exec.cancel_order(mr.working_bid.exchange_order_id)
                        if mr.working_ask and mr.working_ask.exchange_order_id and not mr.working_ask.exchange_order_id.startswith('SIMULATED:'):
                            self.exec.cancel_order(mr.working_ask.exchange_order_id)
                    except Exception as e:
                        logger.error(f"Failed to cancel orders during circuit breaker activation for {market}: {e}")
                
                mr.working_bid = None
                mr.working_ask = None
                return True
        
        return False

    def _apply_market_diffs(self, removed_markets: Set[str], added_markets: Set[str]) -> None:
        """
        Handle changes in active market set:
        - For removed markets: cancel orders (if live) and clear state
        - For added markets: initialize state if needed
        """
        # Handle removed markets
        for m in removed_markets:
            logger.warning(f"market_removed: {m}")
            mr = self.store.get_market(m)
            
            # Cancel working orders if trading enabled
            if self.config.trading_enabled:
                try:
                    # Cancel bid
                    if mr.working_bid and mr.working_bid.exchange_order_id:
                        try:
                            cancel_id = mr.working_bid.exchange_order_id
                            if not cancel_id.startswith('SIMULATED:'):
                                self.exec.cancel_order(cancel_id)
                                logger.info(json_msg({"event": "market_removal_cancel_bid", "market": m, "exchange_order_id": cancel_id}))
                        except Exception as e:
                            logger.error(f"Failed to cancel bid for {m}: {e}")
                    
                    # Cancel ask
                    if mr.working_ask and mr.working_ask.exchange_order_id:
                        try:
                            cancel_id = mr.working_ask.exchange_order_id
                            if not cancel_id.startswith('SIMULATED:'):
                                self.exec.cancel_order(cancel_id)
                                logger.info(json_msg({"event": "market_removal_cancel_ask", "market": m, "exchange_order_id": cancel_id}))
                        except Exception as e:
                            logger.error(f"Failed to cancel ask for {m}: {e}")
                except Exception as e:
                    logger.error(f"Error cancelling orders for removed market {m}: {e}")
            
            # Clear working orders from state
            mr.working_bid = None
            mr.working_ask = None
            
            # Clear order registries for this market
            to_remove_exchange_ids = [oid for oid, oref in self.state.order_by_exchange_id.items() if oref.market_ticker == m]
            to_remove_client_ids = [cid for cid, oref in self.state.order_by_client_id.items() if oref.market_ticker == m]
            
            for oid in to_remove_exchange_ids:
                del self.state.order_by_exchange_id[oid]
            for cid in to_remove_client_ids:
                del self.state.order_by_client_id[cid]
            
            logger.info(json_msg({"event": "market_state_cleared", "market": m, "registry_exchange_ids_removed": len(to_remove_exchange_ids), "registry_client_ids_removed": len(to_remove_client_ids)}))
        
        # Handle added markets (no action needed; state will be initialized on first loop)
        for m in added_markets:
            logger.info(json_msg({"event": "market_added", "market": m}))
            # Just ensure the lock exists
            if m not in self.market_locks:
                self.market_locks[m] = asyncio.Lock()

    async def _apply_market_diffs_async(self, added_markets: Set[str]) -> None:
        """
        Async handler for market additions - subscribe to new markets on WebSocket
        """
        for m in added_markets:
            if getattr(self, '_md_source', None) == 'ws' and self.md and hasattr(self.md, 'rt') and self.md.rt:
                try:
                    await self.md.rt.subscribe_markets([m])
                    logger.info(json_msg({"event": "market_added_ws_subscribed", "market": m}))
                except Exception as e:
                    logger.error(json_msg({"event": "market_added_ws_subscribe_failed", "market": m, "error": str(e)}))

    async def run_once(self):
        # Hot-reload: check for market set changes
        active_markets = self.market_selector.get_active_markets()
        if active_markets != self._last_active_markets:
            removed = self._last_active_markets - active_markets
            added = active_markets - self._last_active_markets
            self._apply_market_diffs(removed, added)
            # Handle async WebSocket resubscription for added markets
            if added:
                await self._apply_market_diffs_async(added)
            self._last_active_markets = active_markets
        
        markets = list(active_markets) if active_markets else list(self.market_selector.get_active_markets())
        tick_size = 0.01
        
        # Use latest reconciled positions snapshot populated by ReconciliationService.
        # This is the SAME data source that writes mm_positions, so risk gating
        # and analytics share one consistent view of position.
        pos_by_ticker = getattr(self, 'last_exchange_positions_by_ticker', {}) or {}

        # Log position source once per minute for audit (and to prove what risk sees).
        now_ts = time.time()
        if now_ts - self._last_position_source_log_ts >= 60:
            self._last_position_source_log_ts = now_ts
            logger.info(json_msg({
                "event": "exchange_positions_snapshot",
                "source": "reconciled_mm_positions",
                "positions_by_ticker": pos_by_ticker,
                "positions_count": len(pos_by_ticker),
            }))
        
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

            # Exchange position from reconciled snapshot: single source of truth.
            ex_pos = int(pos_by_ticker.get(m, mr.inventory or 0))
            mr.exchange_position = ex_pos  # store for visibility / logs only

            # Proper YES book validation: require both bid and ask, both in (0, 1), bid < ask
            if bb is None or ba is None:
                logger.info(json_msg({"event":"skip_no_book", "market": m}))
                continue

            try:
                bb_val = float(bb)
                ba_val = float(ba)
                # WS placeholder ask protection: treat 0.01 ask as missing when bid is meaningful
                if ba_val <= 0.011 and bb_val >= 0.20:
                    logger.error(json_msg({"event": "skip_bogus_yes_ask", "market": m, "bb": bb, "ba": ba}))
                    continue
                if not (0.0 < bb_val < 1.0) or not (0.0 < ba_val < 1.0):
                    logger.error(json_msg({"event":"skip_bad_yes_range", "market": m, "bb": bb, "ba": ba}))
                    continue
                if bb_val >= ba_val:
                    logger.error(json_msg({"event":"skip_inverted_yes_book", "market": m, "bb": bb, "ba": ba}))
                    continue
            except (ValueError, TypeError):
                logger.error(json_msg({"event":"skip_bad_yes_type", "market": m, "bb": bb, "ba": ba}))
                continue

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
                    "exchange_position": ex_pos,
                    "md_yes_bb": bb,
                    "md_yes_ba": ba,
                    "md_no_bb": mr.no_bb_px,
                    "md_no_ba": mr.no_ba_px,
                }))

            # Quote computation: use both bid and ask from market
            edge_ticks = int(self.config.edge_ticks)
            tick_size = 0.01

            bid_px = max(0.01, min(0.99, float(bb) - edge_ticks * tick_size))
            ask_px = max(0.01, min(0.99, float(ba) + edge_ticks * tick_size))

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

            # Global risk check here is ONLY for kill / stale gating.
            # Position-cap enforcement (including reduce-only logic) happens per-side
            # using explicit intended_delta values for each order.
            allowed, reason = self.risk.check_market(mr, exchange_position=ex_pos, intended_delta=0.0)
            
            # Check if market is disabled by circuit breaker
            if mr.disabled_until_ms is not None and now < mr.disabled_until_ms:
                logger.warning(json_msg({
                    "event": "quote_skip_circuit_breaker_disabled",
                    "market": m,
                    "disabled_until_ms": mr.disabled_until_ms,
                }))
                continue

            logger.info(json_msg({
                "event": "quote_eval",
                "market": m,
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
            # Compute mid and spread from actual market mid
            mid = (float(bb) + float(ba)) / 2.0
            spread = float(target.ask_px) - float(target.bid_px)
            self._log_decision(decision_id, m, float(bb), float(ba), mid, spread, target, mr.inventory, mr.inventory)

            if not allowed:
                # Hard gates (KILL_SWITCH/STALE_MARKET) still stop quoting entirely.
                logger.info(json_msg({
                    "event": "risk_block_global",
                    "market": m,
                    "exchange_position": ex_pos,
                    "reason": reason,
                }))
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

                # Minimum replace interval per market.
                # Use configured quote_refresh_ms so MM_QUOTE_REFRESH_MS env is honored.
                min_replace_ms = int(self.config.quote_refresh_ms)
                now = now_ms()

                # Helper to create deterministic client_order_id
                # Keep it short (32 chars max) to avoid Kalshi API client_order_id constraints.
                # Metadata (instance, market, side, timestamp) stored in state/logs instead.
                def make_client_id(side_char: str) -> str:
                    # Format: B_<30-char-hex> or A_<30-char-hex> = 32 chars total
                    return f"{side_char}_{uuid4_hex()[:30]}"

                # BID side replace semantics
                if target.bid_px is not None:
                    # Per-side risk check: BID = BUY YES = +delta
                    bid_intended_delta = target.bid_sz
                    bid_allowed, bid_reason = self.risk.check_market(mr, exchange_position=ex_pos, intended_delta=bid_intended_delta)
                    
                    if not bid_allowed:
                        logger.info(json_msg({
                            "event": "risk_block_bid_side",
                            "market": m,
                            "exchange_position": ex_pos,
                            "bid_intended_delta": bid_intended_delta,
                            "reason": bid_reason
                        }))
                    else:
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
                                # Apply min_reprice_ticks threshold: only reprice if movement is >= threshold
                                min_reprice_cents = self.config.min_reprice_ticks * 100
                                price_diff = abs(new_price_cents - wo.price_cents)
                                
                                if int(wo.size) != int(target.bid_sz):
                                    # Size changed: always reprice
                                    need_replace = True
                                elif price_diff >= min_reprice_cents:
                                    # Price moved enough: reprice
                                    need_replace = True
                                else:
                                    # Same price/size or tiny move: no-op, don't cancel/replace
                                    logger.info(json_msg({
                                        "event": "skip_same_quote",
                                        "market": m,
                                        "side": "BID",
                                        "old_price_cents": wo.price_cents,
                                        "new_price_cents": new_price_cents,
                                        "price_diff_cents": price_diff,
                                        "min_reprice_cents": min_reprice_cents,
                                    }))
                                    need_replace = False

                            if need_replace:
                                # ensure status/exch_id are always defined (paper or live)
                                status = 'SIMULATED' if not self.config.trading_enabled else 'PENDING'
                                exch_id = None

                                # if existing working order, cancel it first
                                if wo is not None:
                                    action_id = uuid4_hex()
                                    cancel_client = wo.client_order_id
                                    api_side = 'yes'
                                    self._log_action(action_id, decision_id, m, cancel_client, 'CANCEL', 'BID', api_side, wo.price_cents / 100.0, wo.price_cents, wo.size, replace_of='')
                                    if not self.config.trading_enabled:
                                        self._log_response(action_id, m, cancel_client, 'SIMULATED', '', '', 0, json.dumps({'simulated': True}))
                                        # In paper mode, treat cancel as immediate and clear local state
                                        try:
                                            wo.status = 'CANCELLED'
                                            if getattr(wo, 'exchange_order_id', None):
                                                self.state.order_by_exchange_id.pop(str(wo.exchange_order_id), None)
                                            self.state.order_by_client_id.pop(getattr(wo, 'client_order_id', ''), None)
                                        except Exception:
                                            logger.exception('failed to cleanup registry on cancel')
                                        mr.working_bid = None
                                    else:
                                        # Use exchange_order_id only; fail loudly if missing
                                        cancel_id = wo.exchange_order_id
                                        if not cancel_id:
                                            logger.error(json_msg({"event":"cancel_missing_exchange_order_id", "market": m, "client_order_id": wo.client_order_id}))
                                            wo.status = 'PENDING_CANCEL'
                                            mr.working_bid = None
                                        else:
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

                                # place new order only when we actually need to replace
                                action_id = uuid4_hex()
                                client_order_id = make_client_id('B')
                                request_json = {
                                    "ticker": m,
                                    "client_order_id": client_order_id,
                                    "count": int(target.bid_sz),
                                    "side": "yes",
                                    "action": "buy",
                                    "type": "limit",
                                    "yes_price": new_price_cents,
                                }
                                self._log_action(action_id, decision_id, m, client_order_id, 'PLACE', 'BID', 'yes', target.bid_px, new_price_cents, target.bid_sz, replace_of=(wo.client_order_id if wo else ''), request_json=request_json)
                                if not self.config.trading_enabled:
                                    logger.info(json_msg({
                                        "event": "paper_place",
                                        "market": m,
                                        "side": "BID",
                                        "px_cents": int(new_price_cents),
                                        "sz": int(target.bid_sz),
                                    }))
                                    mr.last_place_bid_ms = now
                                    sim_exch = f"SIMULATED:{client_order_id}"
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
                                    last['bid_px'] = target.bid_px
                                    last['ts_ms'] = now
                                    try:
                                        self.state.order_by_client_id[client_order_id] = OrderRef(market_ticker=m, internal_side='BID', decision_id=decision_id, client_order_id=client_order_id)
                                    except Exception:
                                        logger.exception('failed to update order registry')
                                else:
                                    resp = await asyncio.to_thread(
                                        self.exec.place_order,
                                        market_ticker=m,
                                        side="yes",
                                        price_cents=new_price_cents,
                                        size=int(target.bid_sz),
                                        client_order_id=client_order_id,
                                        action="buy",
                                    )
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

                                    self._log_response(action_id, m, client_order_id, status, exch_id, reject_reason, resp.get('latency_ms', 0), resp.get('raw', ''))

                                    if status != 'ACK':
                                        if self.config.trading_enabled:
                                            # Check circuit breaker for not_found
                                            if self._check_and_apply_not_found_circuit_breaker(m, reject_reason, mr):
                                                logger.warning(json_msg({"event": "place_skip_circuit_breaker_active", "market": m, "side": "BID"}))
                                                continue
                                            
                                            # 2s cooldown per reject per market (tune)
                                            mr.cooldown_until_ms = now_ms() + 2000
                                            mr.rejects_rolling_counter += 1
                                            logger.error(json_msg({"event":"place_reject", "market": m, "side": "BID", "reason": reject_reason, "cooldown_ms": 2000}))

                                        # hard kill if reject spike enabled
                                        if self.config.kill_on_reject_spike and mr.rejects_rolling_counter >= self.config.max_rejects_per_min:
                                            logger.error(json_msg({"event":"reject_spike_kill", "market": m, "rejects": mr.rejects_rolling_counter}))
                                            self._running = False
                                        # On reject, skip updating working order and try again after cooldown
                                        continue

                                # update working order (only if ACK and we actually placed)
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
                    # Per-side risk check: ASK = BUY NO = -delta (reduces YES exposure)
                    ask_intended_delta = -target.ask_sz
                    ask_allowed, ask_reason = self.risk.check_market(mr, exchange_position=ex_pos, intended_delta=ask_intended_delta)
                    
                    if not ask_allowed:
                        logger.info(json_msg({
                            "event": "risk_block_ask_side",
                            "market": m,
                            "exchange_position": ex_pos,
                            "ask_intended_delta": ask_intended_delta,
                            "reason": ask_reason
                        }))
                    else:
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
                                # Apply min_reprice_ticks threshold: only reprice if movement is >= threshold
                                min_reprice_cents = self.config.min_reprice_ticks * 100
                                price_diff = abs(new_price_cents - wo.price_cents)
                                
                                if int(wo.size) != int(target.ask_sz):
                                    # Size changed: always reprice
                                    need_replace = True
                                elif price_diff >= min_reprice_cents:
                                    # Price moved enough: reprice
                                    need_replace = True
                                else:
                                    # Same price/size or tiny move: no-op, don't cancel/replace
                                    logger.info(json_msg({
                                        "event": "skip_same_quote",
                                        "market": m,
                                        "side": "ASK",
                                        "old_price_cents": wo.price_cents,
                                        "new_price_cents": new_price_cents,
                                        "price_diff_cents": price_diff,
                                        "min_reprice_cents": min_reprice_cents,
                                    }))
                                    need_replace = False

                            if need_replace:
                                # ensure status/exch_id are always defined (paper or live)
                                status = 'SIMULATED' if not self.config.trading_enabled else 'PENDING'
                                exch_id = None

                                if wo is not None:
                                    action_id = uuid4_hex()
                                    cancel_client = wo.client_order_id
                                    self._log_action(action_id, decision_id, m, cancel_client, 'CANCEL', 'ASK', 'no', wo.price_cents / 100.0, wo.price_cents, wo.size, replace_of='')
                                    # simulate cancel in paper mode
                                    if not self.config.trading_enabled:
                                        self._log_response(action_id, m, cancel_client, 'SIMULATED', '', '', 0, json.dumps({'simulated': True}))
                                        # In paper mode, treat cancel as immediate and clear local state
                                        try:
                                            wo.status = 'CANCELLED'
                                            if getattr(wo, 'exchange_order_id', None):
                                                self.state.order_by_exchange_id.pop(str(wo.exchange_order_id), None)
                                            self.state.order_by_client_id.pop(getattr(wo, 'client_order_id', ''), None)
                                        except Exception:
                                            logger.exception('failed to cleanup registry on cancel')
                                        mr.working_ask = None
                                    else:
                                        # Use exchange_order_id only; fail loudly if missing
                                        cancel_id = wo.exchange_order_id
                                        if not cancel_id:
                                            logger.error(json_msg({"event":"cancel_missing_exchange_order_id", "market": m, "client_order_id": wo.client_order_id}))
                                            wo.status = 'PENDING_CANCEL'
                                            mr.working_ask = None
                                        else:
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

                                # place new order only when we actually need to replace
                                action_id = uuid4_hex()
                                client_order_id = make_client_id('A')
                                # ASK side now places BUY NO (not SELL YES)
                                # Convert YES ask price to NO bid price via complement
                                no_price_cents = complement_100(new_price_cents)
                                request_json = {
                                    "ticker": m,
                                    "client_order_id": client_order_id,
                                    "count": int(target.ask_sz),
                                    "side": "no",
                                    "action": "buy",
                                    "type": "limit",
                                    "no_price": no_price_cents,
                                }
                                self._log_action(action_id, decision_id, m, client_order_id, 'PLACE', 'ASK', 'no', no_price_cents / 100.0, no_price_cents, target.ask_sz, replace_of=(wo.client_order_id if wo else ''), request_json=request_json)
                                if not self.config.trading_enabled:
                                    logger.info(json_msg({
                                        "event": "paper_place",
                                        "market": m,
                                        "side": "ASK",
                                        "px_cents": int(no_price_cents),
                                        "sz": int(target.ask_sz),
                                    }))
                                    mr.last_place_ask_ms = now
                                    sim_exch = f"SIMULATED:{client_order_id}"
                                    wo_new = WorkingOrder(
                                        client_order_id=client_order_id,
                                        exchange_order_id=sim_exch,
                                        side='ASK',
                                        price_cents=no_price_cents,
                                        size=target.ask_sz,
                                        status='ACKED',
                                        placed_ts_ms=now,
                                        remaining_size=target.ask_sz,
                                        last_update_ts_ms=now,
                                    )
                                    mr.working_ask = wo_new
                                    last['ask_px'] = target.ask_px
                                    last['ts_ms'] = now
                                    try:
                                        self.state.order_by_client_id[client_order_id] = OrderRef(market_ticker=m, internal_side='ASK', decision_id=decision_id, client_order_id=client_order_id)
                                    except Exception:
                                        logger.exception('failed to update order registry')
                                else:
                                    resp = await asyncio.to_thread(
                                        self.exec.place_order,
                                        market_ticker=m,
                                        side="no",
                                        price_cents=no_price_cents,
                                        size=int(target.ask_sz),
                                        client_order_id=client_order_id,
                                        action="buy",
                                    )
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

                                    self._log_response(action_id, m, client_order_id, status, exch_id, reject_reason, resp.get('latency_ms', 0), resp.get('raw', ''))
                                    
                                    if status != 'ACK':
                                        if self.config.trading_enabled:
                                            # Check circuit breaker for not_found
                                            if self._check_and_apply_not_found_circuit_breaker(m, reject_reason, mr):
                                                logger.warning(json_msg({"event": "place_skip_circuit_breaker_active", "market": m, "side": "ASK"}))
                                                continue
                                            
                                            # 2s cooldown per reject per market (tune)
                                            mr.cooldown_until_ms = now_ms() + 2000
                                            mr.rejects_rolling_counter += 1
                                            logger.error(json_msg({"event":"place_reject", "market": m, "side": "ASK", "reason": reject_reason, "cooldown_ms": 2000}))

                                            # hard kill if reject spike enabled
                                            if self.config.kill_on_reject_spike and mr.rejects_rolling_counter >= self.config.max_rejects_per_min:
                                                logger.error(json_msg({"event":"reject_spike_kill", "market": m, "rejects": mr.rejects_rolling_counter}))
                                                self._running = False
                                        else:
                                            logger.info(json_msg({
                                                "event": "paper_place_skip_reject_accounting",
                                                "market": m,
                                                "side": "ASK",
                                                "why": "trading_disabled",
                                            }))

                                        # do NOT attempt next cycle until cooldown expires
                                        continue

                                # update working order (only if ACK and we actually placed)
                                mr.last_place_ask_ms = now
                                wo_new = None
                                if status == 'ACK' or (not self.config.trading_enabled and status == 'SIMULATED'):
                                    # for simulated, set exchange id to a simulated token
                                    sim_exch = exch_id or (f"SIMULATED:{client_order_id}" if not self.config.trading_enabled else None)
                                    wo_new = WorkingOrder(
                                        client_order_id=client_order_id,
                                        exchange_order_id=sim_exch,
                                        side='ASK',
                                        price_cents=no_price_cents,
                                        size=target.ask_sz,
                                        status='ACKED',
                                        placed_ts_ms=now,
                                        remaining_size=target.ask_sz,
                                        last_update_ts_ms=now,
                                    )
                                    mr.working_ask = wo_new
                                    logger.info(json_msg({"event":"wo_set","market":m,"side":"ASK","client_order_id":client_order_id,"exchange_order_id":sim_exch,"price_cents":no_price_cents,"size":target.ask_sz}))
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

        # CRITICAL FIX: Startup safety - check if already over position cap
        # If so, disable trading until manually acknowledged
        if self.config.trading_enabled:
            try:
                pos_by_ticker = self._get_exchange_pos_by_ticker()
                max_pos = self.config.max_pos
                over_cap_markets = []
                for ticker, pos in pos_by_ticker.items():
                    if abs(pos) > max_pos:
                        over_cap_markets.append((ticker, pos))
                
                if over_cap_markets:
                    logger.error(json_msg({
                        "event": "startup_position_cap_breach",
                        "status": "TRADING_DISABLED",
                        "reason": "Already holding positions over max_pos cap at startup",
                        "max_pos": max_pos,
                        "breached_markets": [
                            {"ticker": t, "position": p} for t, p in over_cap_markets
                        ],
                        "action_required": "Manually review positions and acknowledge in logs to re-enable trading"
                    }))
                    self.config.trading_enabled = False
            except Exception:
                logger.exception('startup position cap check failed')

        # Startup safety: cancel all resting orders on configured tickers if trading enabled
        if self.config.trading_enabled and os.getenv("MM_CANCEL_ALL_ON_START", "1") == "1":
            startup_markets = list(self.market_selector.get_active_markets())
            for m in startup_markets:
                try:
                    orders = self.exec.get_open_orders(ticker=m, limit=500)
                    cancel_count = 0
                    for o in orders:
                        oid = o.get("order_id")
                        if oid and o.get("status") == "resting" and int(o.get("remaining_count") or 0) > 0:
                            self.exec.cancel_order(oid)
                            cancel_count += 1
                    if cancel_count > 0:
                        logger.info(json_msg({"event":"startup_cancel_all_done","market":m,"n":cancel_count}))
                except Exception:
                    logger.exception("startup cancel all failed for market %s", m)
        # Start market data provider and wait for initial BBOs where applicable
        try:
            # Get initial markets from MarketSelector (config file or env var)
            initial_markets = list(self.market_selector.get_active_markets())
            if not initial_markets:
                # Fallback: if selector returns empty, use empty list (graceful degradation)
                initial_markets = []
                
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
                    await self.md.start(initial_markets)
                except TypeError:
                    # older providers may not accept markets param
                    await self.md.start()
            wait_ms = int(os.getenv('MM_MD_WAIT_MS', '5000'))
            if hasattr(self.md, 'wait_ready'):
                ok = await self.md.wait_ready(initial_markets, timeout_s=(wait_ms / 1000.0))
            elif hasattr(self.md, 'wait_for_initial_bbo'):
                ok = await self.md.wait_for_initial_bbo(initial_markets, timeout_ms=wait_ms)
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
