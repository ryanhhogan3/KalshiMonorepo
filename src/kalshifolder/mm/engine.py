import asyncio
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

logger = logging.getLogger(__name__)


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
        self.md = ClickHouseMarketDataProvider(
            self.config.ch_url,
            user=self.config.ch_user,
            pwd=self.config.ch_pwd,
            db=self.config.ch_db,
        )
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
            logger.exception('market data batch failed; marking markets stale')
            # mark all markets stale
            for m in markets:
                mr = self.store.get_market(m)
                mr.kill_stale = True
            return

        now = now_ms()
        for m in markets:
            mr = self.store.get_market(m)
            md = batch.get(m)
            if not md:
                mr.kill_stale = True
                logger.info(json_msg({'event': 'stale_market', 'market': m}))
                continue
            # update runtime state
            mr.last_bb_px = md.get('bb_px')
            mr.last_bb_sz = md.get('bb_sz')
            mr.last_ba_px = md.get('ba_px')
            mr.last_ba_sz = md.get('ba_sz')
            mr.last_ts_ms = md.get('ts_ms')
            # staleness
            if mr.last_ts_ms is None or (now - mr.last_ts_ms) > self.config.max_level_age_ms:
                mr.kill_stale = True
                logger.info(json_msg({'event': 'stale_market_age', 'market': m, 'last_ts_ms': mr.last_ts_ms}))
                continue

            # ensure lock exists
            if m not in self.market_locks:
                self.market_locks[m] = asyncio.Lock()
            last = self.last_sent.setdefault(m, {'bid_px': None, 'ask_px': None, 'ts_ms': 0})

            bb = mr.last_bb_px
            ba = mr.last_ba_px
            if bb is None or ba is None:
                mr.kill_stale = True
                logger.info(json_msg({'event': 'no_book', 'market': m}))
                # cancel working orders if any (not implemented here)
                continue

            target = compute_quotes(bb, ba, mr.inventory, self.config.max_pos, self.config.edge_ticks, tick_size, self.config.size)
            allowed, reason = self.risk.check_market(mr)
            decision_id = uuid4_hex()
            mid = (bb + ba) / 2.0
            spread = ba - bb
            self._log_decision(decision_id, m, bb, ba, mid, spread, target, mr.inventory, mr.inventory)
            if not allowed:
                logger.info(json_msg({'event': 'risk_block', 'market': m, 'reason': reason}))
                continue

            async with self.market_locks[m]:
                # cancellation timeout (ms)
                cancel_timeout_ms = 1000

                # Helper to create deterministic client_order_id
                def make_client_id(side_char: str) -> str:
                    return f"{self.state.instance_id}:{m}:{side_char}:{now_ms()}:{uuid4_hex()[:8]}"

                # BID side replace semantics
                if target.bid_px is not None:
                    wo = mr.working_bid
                    new_price_cents = int(round(target.bid_px * 100))
                    need_replace = False
                    if wo is None:
                        need_replace = True
                    else:
                        # replace if price or size changed beyond thresholds
                        if wo.price_cents != new_price_cents or int(wo.size) != int(target.bid_sz):
                            need_replace = True

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
                                resp = await asyncio.to_thread(self.exec.cancel_order, cancel_client)
                            status = resp.get('status', 'ERROR')
                            exch_id = resp.get('exchange_order_id')
                            if not self.config.trading_enabled:
                                self._log_response(action_id, m, cancel_client, 'SIMULATED', '', '', 0, json.dumps({'simulated': True}))
                            else:
                                self._log_response(action_id, m, cancel_client, status, exch_id, '', resp.get('latency_ms', 0), resp.get('raw', ''))

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
                        # log response but mark simulated separately
                        if not self.config.trading_enabled:
                            self._log_response(action_id, m, client_order_id, 'SIMULATED', '', '', 0, json.dumps({'simulated': True}))
                        else:
                            self._log_response(action_id, m, client_order_id, status, exch_id, '', resp.get('latency_ms', 0), resp.get('raw', ''))

                        # update working order
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
                    wo = mr.working_ask
                    new_price_cents = int(round(target.ask_px * 100))
                    need_replace = False
                    if wo is None:
                        need_replace = True
                    else:
                        if wo.price_cents != new_price_cents or int(wo.size) != int(target.ask_sz):
                            need_replace = True

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
                                resp = await asyncio.to_thread(self.exec.cancel_order, cancel_client)
                            status = resp.get('status', 'ERROR')
                            exch_id = resp.get('exchange_order_id')
                            if not self.config.trading_enabled:
                                self._log_response(action_id, m, cancel_client, 'SIMULATED', '', '', 0, json.dumps({'simulated': True}))
                            else:
                                self._log_response(action_id, m, cancel_client, status, exch_id, '', resp.get('latency_ms', 0), resp.get('raw', ''))
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
                        if not self.config.trading_enabled:
                            self._log_response(action_id, m, client_order_id, 'SIMULATED', '', '', 0, json.dumps({'simulated': True}))
                        else:
                            self._log_response(action_id, m, client_order_id, status, exch_id, '', resp.get('latency_ms', 0), resp.get('raw', ''))
                        if status == 'ACK' or (not self.config.trading_enabled and status == 'SIMULATED'):
                            wo_new = WorkingOrder(
                                client_order_id=client_order_id,
                                exchange_order_id=(exch_id or (f"SIMULATED:{client_order_id}" if not self.config.trading_enabled else None)),
                                side='ASK',
                                price_cents=new_price_cents,
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
                                if exch_id:
                                    self.state.order_by_exchange_id[str(exch_id)] = OrderRef(market_ticker=m, internal_side='ASK', decision_id=decision_id, client_order_id=client_order_id)
                                self.state.order_by_client_id[client_order_id] = OrderRef(market_ticker=m, internal_side='ASK', decision_id=decision_id, client_order_id=client_order_id)
                            except Exception:
                                logger.exception('failed to update order registry')
                        else:
                            mr.rejects_rolling_counter += 1

    async def run(self):
        self._running = True
        await self.ensure_schema()
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
        recon_task = asyncio.create_task(self.reconcile_loop())
        try:
            while self._running:
                await self.run_once()
                await asyncio.sleep(self.config.poll_ms / 1000.0)
        except asyncio.CancelledError:
            pass
        finally:
            self._running = False
            recon_task.cancel()

    def stop(self):
        self._running = False
