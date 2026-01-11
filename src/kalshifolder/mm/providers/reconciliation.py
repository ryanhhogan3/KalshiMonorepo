import logging
from typing import Optional, Dict, Any
import time
from ..utils.id import uuid4_hex
from ..utils.logging import json_msg

logger = logging.getLogger(__name__)


class ReconciliationService:
    def __init__(self, engine, exec_provider, ch_writer=None):
        # engine: Engine instance (allows calling engine._log_action/_log_response and accessing state)
        self.engine = engine
        self.exec = exec_provider
        self.ch = ch_writer
        self._last_fills_ts = 0

    def _normalize_order(self, o: Dict[str, Any]) -> Dict[str, Any]:
        # tolerate multiple key names
        return {
            'market_ticker': o.get('market_ticker') or o.get('market') or o.get('marketTicker'),
            'exchange_order_id': o.get('exchange_order_id') or o.get('order_id') or o.get('id'),
            'client_order_id': o.get('client_order_id') or o.get('clientOrderId') or o.get('client_id'),
            'side': o.get('side') or o.get('yes_no') ,
            'price_cents': int(o.get('price_cents') or o.get('price') or 0),
            'remaining_size': float(o.get('remaining_size') or o.get('remaining') or o.get('size') or 0),
        }

    def _normalize_fill(self, f: Dict[str, Any]) -> Dict[str, Any]:
        # Extract size/count from fill
        # Kalshi API uses 'count' for number of contracts
        size = f.get('count') or f.get('size') or f.get('qty') or 0
        try:
            size = float(size)
        except (ValueError, TypeError):
            size = 0.0
        
        # Skip fills with 0 or negative size
        if size <= 0:
            logger.debug(json_msg({"event": "fill_skipped_zero_size", "exchange_order_id": f.get('exchange_order_id'), "size": size}))
            return None
        
        # Extract price_cents
        price_cents = int(f.get('price') or f.get('price_cents') or 0)
        
        # Hard validity gate: price_cents must be in [1, 99]
        if price_cents < 1 or price_cents > 99:
            logger.warning(json_msg({
                "event": "fill_skipped_invalid_price",
                "exchange_order_id": f.get('exchange_order_id'),
                "price_cents": price_cents,
            }))
            return None
        
        # Hard validity gate: exchange_order_id must be set
        exchange_order_id = f.get('exchange_order_id') or f.get('order_id') or f.get('id')
        if not exchange_order_id:
            logger.warning(json_msg({
                "event": "fill_skipped_missing_order_id",
                "raw_keys": list(f.keys()),
            }))
            return None
        
        # Extract timestamp from payload (must come from API, not now())
        ts = f.get('ts') or f.get('timestamp') or f.get('time') or f.get('created_at')
        if not ts:
            logger.warning(json_msg({
                "event": "fill_skipped_missing_timestamp",
                "exchange_order_id": exchange_order_id,
            }))
            return None
        
        try:
            ts = int(ts)
        except (ValueError, TypeError):
            logger.warning(json_msg({
                "event": "fill_skipped_unparseable_timestamp",
                "exchange_order_id": exchange_order_id,
                "ts": ts,
            }))
            return None
        
        return {
            'ts': ts,
            'market_ticker': f.get('market_ticker') or f.get('market') or f.get('marketTicker'),
            'exchange_order_id': exchange_order_id,
            'client_order_id': f.get('client_order_id') or f.get('clientOrderId') or f.get('client_id'),
            'side': f.get('side'),
            'price_cents': price_cents,
            'size': size,  # Now using count field instead of size
            'raw': f,
        }

    def reconcile_open_orders(self):
        # fetch exchange open orders
        try:
            exch_orders = self.exec.get_open_orders() or []
        except Exception:
            logger.exception('get_open_orders failed')
            exch_orders = []

        # group by market and side
        by_market = {}
        for o in exch_orders:
            no = self._normalize_order(o)
            m = no['market_ticker']
            if not m:
                continue
            by_market.setdefault(m, []).append(no)

        # ensure we check markets even if the exchange returns no orders for them
        all_markets = set(list(by_market.keys()) + list(self.engine.state.markets.keys()))
        for market in all_markets:
            orders = by_market.get(market, [])
            mr = self.engine.store.get_market(market)

            # map exchange ids for quick lookup
            exch_ids = {o['exchange_order_id']: o for o in orders if o.get('exchange_order_id')}

            # A. detect missing local orders
            for side_attr in ('working_bid', 'working_ask'):
                wo = getattr(mr, side_attr)
                if wo and getattr(wo, 'status', None) == 'ACKED':
                    exch_id = getattr(wo, 'exchange_order_id', None)
                    if exch_id and exch_id not in exch_ids:
                        # If this is a simulated local-only order (SIMULATED:...) and
                        # trading is disabled, do not clear it — paper mode must not
                        # touch the account or treat local simulations as exchange state.
                        if isinstance(exch_id, str) and exch_id.startswith('SIMULATED:') and getattr(self.engine, 'config', None) and not getattr(self.engine.config, 'trading_enabled', True):
                            logger.info('recon: ignoring simulated local order during paper mode', extra={'market': market, 'side': side_attr, 'exchange_order_id': exch_id})
                            continue
                        # local believes order is live but exchange doesn't
                        logger.warning('recon: local order missing on exchange', extra={'market': market, 'side': side_attr, 'exchange_order_id': exch_id})
                        # clear local
                        setattr(mr, side_attr, None)
                        # write anomaly row if ch available
                        if self.ch:
                            try:
                                row = {
                                    'ts': time.strftime('%Y-%m-%d %H:%M:%S'),
                                    'engine_instance_id': self.engine.state.instance_id,
                                    'market_ticker': market,
                                    'kind': 'missing_local_on_exchange',
                                    'exchange_order_id': exch_id,
                                }
                                self.ch.insert('mm_anomalies', [row])
                            except Exception:
                                logger.exception('failed to write anomaly')

            # B. cancel stray exchange orders
            for o in orders:
                known = False
                # compare with working orders
                if mr.working_bid and (mr.working_bid.exchange_order_id == o.get('exchange_order_id') or mr.working_bid.client_order_id == o.get('client_order_id')):
                    known = True
                if mr.working_ask and (mr.working_ask.exchange_order_id == o.get('exchange_order_id') or mr.working_ask.client_order_id == o.get('client_order_id')):
                    known = True

                if not known:
                    # in paper/dry-run mode we do not cancel stray orders
                    # Only skip cancels when the engine has a config and trading is explicitly disabled.
                    if getattr(self.engine, 'config', None) and not getattr(self.engine.config, 'trading_enabled', True):
                        logger.info('recon: skipping cancel of stray order due to trading disabled', extra={'market': market, 'exchange_order_id': o.get('exchange_order_id')})
                        continue
                    # If client_order_id absent or doesn't include engine id, or unknown, cancel it
                    client_id = o.get('client_order_id')
                    # cancel by client_id when possible, else by exchange id path
                    cancel_target = client_id or o.get('exchange_order_id')
                    logger.info('recon: cancelling stray exchange order', extra={'market': market, 'exchange_order_id': o.get('exchange_order_id'), 'client_order_id': client_id})
                    try:
                        # log CANCEL action via engine if possible
                        if hasattr(self.engine, '_log_action'):
                            aid = uuid4_hex()
                            # best-effort log
                            try:
                                self.engine._log_action(aid, '', market, cancel_target, 'CANCEL', 'UNKNOWN', o.get('side'), o.get('price_cents') / 100.0, o.get('price_cents'), o.get('remaining_size'), request_json={})
                            except Exception:
                                logger.exception('failed logging cancel action')
                        resp = self.exec.cancel_order(cancel_target)
                        # log response
                        if hasattr(self.engine, '_log_response'):
                            try:
                                self.engine._log_response(aid, market, cancel_target, resp.get('status', 'ERROR'), resp.get('exchange_order_id'), '', resp.get('latency_ms', 0), resp.get('raw', ''))
                            except Exception:
                                logger.exception('failed logging cancel response')
                    except Exception:
                        logger.exception('failed cancelling stray order')

            # C. multiple orders per side -> cancel extras
            # group by side intent using normalized side (yes/no)
            sides = {}
            for o in orders:
                s = o.get('side') or 'unknown'
                sides.setdefault(s, []).append(o)
            for s, lst in sides.items():
                if len(lst) > 1:
                    # keep the most recent (best effort: by price or first), cancel others
                    keep = lst[0]
                    for extra in lst[1:]:
                        try:
                            cancel_target = extra.get('client_order_id') or extra.get('exchange_order_id')
                            logger.info('recon: cancelling duplicate side order', extra={'market': market, 'side': s, 'exchange_order_id': extra.get('exchange_order_id')})
                            self.exec.cancel_order(cancel_target)
                        except Exception:
                            logger.exception('failed cancelling duplicate')

    def fetch_and_apply_fills(self):
        fills = []
        try:
            fills = self.exec.get_fills(self._last_fills_ts) or []
        except Exception:
            logger.exception('get_fills failed')
            return []

        # Defensive normalization: ensure fills is a list of dicts
        # get_fills() should return a list, but handle dict responses from API
        if isinstance(fills, dict):
            # Extract fills array from dict (handle various API response formats)
            fills = fills.get("fills") or fills.get("data") or fills.get("items") or []
        
        if isinstance(fills, str):
            logger.error("get_fills returned str; dropping: %r", fills[:200])
            fills = []
        elif not isinstance(fills, list):
            logger.error("get_fills returned %s; expected list", type(fills))
            fills = []
        
        # Ensure all items are dicts
        fills = [f for f in fills if isinstance(f, dict)]

        if not fills:
            return []
        
        # DEBUG: Log first fill JSON structure for field mapping validation
        if fills:
            logger.info(json_msg({"event": "raw_fill_sample", "fill": fills[0], "fill_keys": list(fills[0].keys())}))

        norm_fills = [self._normalize_fill(f) for f in fills]
        # Filter out None values (skipped fills with zero size, invalid price, missing fields)
        norm_fills = [f for f in norm_fills if f is not None]
        
        if not norm_fills:
            return []
        
        # Get engine metadata for fills
        engine_instance_id = getattr(self.engine, 'state', None) and getattr(self.engine.state, 'instance_id', '') or ''
        engine_version = getattr(self.engine, 'state', None) and getattr(self.engine.state, 'version', 'dev') or 'dev'
        
        # write to CH
        if self.ch:
            rows = []
            for f in norm_fills:
                # Look up decision_id from engine's order registries
                decision_id = None
                if self.engine:
                    exch_id = f.get('exchange_order_id')
                    if exch_id and exch_id in self.engine.state.order_by_exchange_id:
                        order_ref = self.engine.state.order_by_exchange_id[exch_id]
                        decision_id = order_ref.decision_id
                
                rows.append({
                    'ts': time.strftime('%Y-%m-%d %H:%M:%S'),
                    'engine_instance_id': engine_instance_id,
                    'engine_version': engine_version,
                    'market_ticker': f['market_ticker'],
                    'exchange_order_id': f['exchange_order_id'],
                    'client_order_id': f['client_order_id'],
                    'side': f['side'],
                    'price_cents': f['price_cents'],
                    'size': f['size'],
                    'decision_id': decision_id,
                    'raw_json': str(f['raw']),
                })
            try:
                self.ch.insert('mm_fills', rows)
                if rows:
                    logger.info(json_msg({
                        "event": "fills_ingested",
                        "count": len(rows),
                        "with_decision_id": sum(1 for r in rows if r.get('decision_id')),
                        "with_side": sum(1 for r in rows if r.get('side')),
                    }))
            except Exception:
                logger.exception('failed to write fills')

        # Attach fills to working orders and update inventory
        for f in norm_fills:
            market = f.get('market_ticker')
            if not market:
                continue
            mr = self.engine.store.get_market(market)
            # find matching working order by exchange or client id
            matched = None
            for side_attr in ('working_bid', 'working_ask'):
                wo = getattr(mr, side_attr)
                if not wo:
                    continue
                if (getattr(wo, 'exchange_order_id', None) and getattr(wo, 'exchange_order_id') == f.get('exchange_order_id')) or (getattr(wo, 'client_order_id', None) and getattr(wo, 'client_order_id') == f.get('client_order_id')):
                    matched = (side_attr, wo)
                    break

            # inventory update rule: inventory defined as YES net position
            # determine if the fill increased YES exposure
            # assume fills include 'side' which is 'yes' or 'no' and include 'action' if available
            side_yes_no = f.get('side')
            # best-effort determine buy/sell
            raw = f.get('raw')
            action = None
            if isinstance(raw, dict):
                action = raw.get('action') or None
                if action is None:
                    # sometimes the fill payload nests raw inside raw
                    inner = raw.get('raw')
                    if isinstance(inner, dict):
                        action = inner.get('action')
            # if action is BUY, then buying YES increases inventory; if BUY and side=='no' then inventory decreases
            delta = 0.0
            try:
                size = float(f.get('size', 0))
            except Exception:
                size = 0.0
            if action and isinstance(action, str):
                a = action.lower()
                if 'buy' in a:
                    if side_yes_no == 'yes':
                        delta = size
                    else:
                        delta = -size
                elif 'sell' in a:
                    if side_yes_no == 'yes':
                        delta = -size
                    else:
                        delta = size
            else:
                # fallback: if client_order_id encodes B/A we can infer, else skip
                coid = f.get('client_order_id') or ''
                if ':B:' in coid or ':b:' in coid:
                    if side_yes_no == 'yes':
                        delta = size
                    else:
                        delta = -size
                elif ':A:' in coid or ':a:' in coid:
                    if side_yes_no == 'yes':
                        delta = -size
                    else:
                        delta = size

                # apply
            if matched:
                side_attr, wo = matched
                # reduce remaining size if tracked
                try:
                    if hasattr(wo, 'remaining_size') and wo.remaining_size is not None:
                        wo.remaining_size = max(0.0, float(wo.remaining_size) - size)
                    else:
                        # fallback adjust size
                        wo.size = max(0.0, float(getattr(wo, 'size', 0)) - size)
                    wo.last_update_ts_ms = int(time.time() * 1000)
                except Exception:
                    pass
                # clear working order if filled
                rem = getattr(wo, 'remaining_size', None)
                if rem is None:
                    rem = getattr(wo, 'size', 0)
                if rem <= 0:
                    # cleanup registries
                    try:
                        if getattr(wo, 'exchange_order_id', None):
                            self.engine.state.order_by_exchange_id.pop(str(wo.exchange_order_id), None)
                        self.engine.state.order_by_client_id.pop(getattr(wo, 'client_order_id', ''), None)
                    except Exception:
                        logger.exception('failed to cleanup registry on fill')
                    setattr(mr, side_attr, None)
            # adjust inventory
            mr.inventory = mr.inventory + delta

            # track last_fills_ts
            self._last_fills_ts = max(self._last_fills_ts, int(f.get('ts', 0)))

        return norm_fills

    def reconcile_positions(self):
        try:
            positions = self.exec.get_positions() or []
        except Exception:
            logger.exception('get_positions failed')
            return []

        # write to CH
        if self.ch and positions:
            rows = []
            for p in positions:
                rows.append({
                    'ts': time.strftime('%Y-%m-%d %H:%M:%S'),
                    'engine_instance_id': self.engine.state.instance_id,
                    'market_ticker': p.get('market_ticker') or p.get('market'),
                    'position': p.get('position') or p.get('qty') or 0,
                    'avg_cost': p.get('avg_cost') or p.get('avgPrice') or 0,
                    'source': 'REST_RECON',
                })
            try:
                self.ch.insert('mm_positions', rows)
            except Exception:
                logger.exception('failed to write positions')

        # overwrite engine state
        for p in positions:
            market = p.get('market_ticker') or p.get('market')
            if not market:
                continue
            mr = self.engine.store.get_market(market)
            mr.inventory = float(p.get('position') or p.get('qty') or 0)

        return positions

    def reconcile_cycle(self):
        # perform open orders reconciliation, fills ingestion, positions overwrite
        try:
            self.reconcile_open_orders()
        except Exception:
            logger.exception('reconcile_open_orders failed')
        try:
            self.fetch_and_apply_fills()
        except Exception:
            logger.exception('fetch_and_apply_fills failed')
        try:
            self.reconcile_positions()
        except Exception:
            logger.exception('reconcile_positions failed')

    def startup_reconcile(self, cancel_engine_tagged_only: bool = True):
        # on startup, fetch open orders and cancel those that have our engine instance tag in client_order_id
        try:
            exch_orders = self.exec.get_open_orders() or []
        except Exception:
            logger.exception('get_open_orders failed')
            exch_orders = []

        # If trading is disabled, do not perform any cancels on startup — keep
        # the invariant that paper/dry-run mode never touches the account.
        if getattr(self.engine, 'config', None) and not getattr(self.engine.config, 'trading_enabled', True):
            logger.info('startup_reconcile: trading disabled, skipping startup cancels')
            return

        for o in exch_orders:
            client = o.get('client_order_id') or o.get('clientOrderId') or ''
            cancel = False
            if cancel_engine_tagged_only:
                if self.engine.state.instance_id in client:
                    cancel = True
            else:
                cancel = True
            if cancel:
                try:
                    target = client or o.get('exchange_order_id') or o.get('order_id')
                    self.exec.cancel_order(target)
                except Exception:
                    logger.exception('startup cancel failed')
