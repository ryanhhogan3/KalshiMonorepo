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
        """
        Normalize a fill from Kalshi API to canonical schema.
        
        CANONICAL FIELD MAPPING:
        - fill_id: fill_id or trade_id
        - exchange_order_id: order_id or orderId
        - size: count or size or qty
        - price_cents: Use side-specific (yes_price if side=yes, no_price if side=no), 
                       fallback to float price * 100
        - timestamp_ms: ts (seconds) * 1000
        - side: yes/no
        - market_ticker: market_ticker or market or ticker
        """
        
        # 1. Extract and validate exchange_order_id (CRITICAL for reconciliation)
        exchange_order_id = f.get('exchange_order_id') or f.get('order_id') or f.get('orderId') or f.get('id')
        if not exchange_order_id or not str(exchange_order_id).strip():
            logger.warning(json_msg({
                "event": "fill_skipped_missing_order_id",
                "raw_keys": list(f.keys()),
            }))
            return None
        
        # 2. Extract and validate fill_id (for idempotency) - MUST ALWAYS HAVE A VALUE
        fill_id = f.get('fill_id') or f.get('trade_id')
        if not fill_id:
            # CRITICAL: Generate synthetic fill_id immediately (not later)
            # This ensures every fill has a stable, deterministic ID for deduping
            import hashlib
            fill_tuple = f"{exchange_order_id}:{side}:{f.get('count', 0)}:{f.get('yes_price', f.get('no_price', f.get('price', 0)))}:{f.get('ts', 0)}"
            fill_id = hashlib.md5(fill_tuple.encode()).hexdigest()
            logger.debug(json_msg({
                "event": "fill_synthetic_id",
                "exchange_order_id": exchange_order_id,
                "synthetic_id": fill_id,
            }))
        
        # Ensure fill_id is never blank
        if not str(fill_id).strip():
            logger.warning(json_msg({
                "event": "fill_skipped_blank_fill_id",
                "exchange_order_id": exchange_order_id,
            }))
            return None
        
        # 3. Extract side (yes/no)
        side = f.get('side')
        if not side:
            logger.warning(json_msg({
                "event": "fill_skipped_missing_side",
                "exchange_order_id": exchange_order_id,
            }))
            return None
        
        # Normalize side
        side = side.lower().strip()
        if side not in ('yes', 'no'):
            logger.warning(json_msg({
                "event": "fill_skipped_invalid_side",
                "exchange_order_id": exchange_order_id,
                "side": side,
            }))
            return None
        
        # 4. Extract size/count from fill
        # Kalshi API uses 'count' for number of contracts
        size = f.get('count') or f.get('size') or f.get('qty') or 0
        try:
            size = float(size)
        except (ValueError, TypeError):
            size = 0.0
        
        # Skip fills with 0 or negative size
        if size <= 0:
            logger.debug(json_msg({
                "event": "fill_skipped_zero_size",
                "exchange_order_id": exchange_order_id,
                "size": size,
            }))
            return None
        
        # 5. Extract price_cents (NON-NEGOTIABLE: SIDE-SPECIFIC MAPPING)
        # Kalshi API provides:
        #   - price: float (0.99) <- universal, but needs conversion
        #   - yes_price: int cents for YES side
        #   - no_price: int cents for NO side
        # RULE: Prefer side-specific price, fallback to float->cents conversion
        price_cents = 0
        
        # Primary: use side-specific price (most reliable)
        if side == 'yes':
            yes_price = f.get('yes_price')
            if yes_price is not None:
                try:
                    price_cents = int(yes_price)
                except (ValueError, TypeError):
                    price_cents = 0
        else:  # side == 'no'
            no_price = f.get('no_price')
            if no_price is not None:
                try:
                    price_cents = int(no_price)
                except (ValueError, TypeError):
                    price_cents = 0
        
        # Fallback: convert float price (dollars) to cents
        if price_cents == 0:
            price_float = f.get('price')
            if price_float is not None:
                try:
                    price_cents = int(float(price_float) * 100)
                except (ValueError, TypeError):
                    price_cents = 0
        
        # Hard validity gate: price_cents must be in [1, 99]
        if price_cents < 1 or price_cents > 99:
            logger.warning(json_msg({
                "event": "fill_skipped_invalid_price",
                "exchange_order_id": exchange_order_id,
                "side": side,
                "price_cents": price_cents,
                "price_raw": f.get('price'),
                "yes_price_raw": f.get('yes_price'),
                "no_price_raw": f.get('no_price'),
            }))
            return None
        
        # 6. Extract timestamp from payload (must come from API, not now())
        # Kalshi API returns 'ts' in SECONDS, convert to milliseconds
        ts = f.get('ts') or f.get('timestamp') or f.get('time') or f.get('created_at')
        if not ts:
            logger.warning(json_msg({
                "event": "fill_skipped_missing_timestamp",
                "exchange_order_id": exchange_order_id,
            }))
            return None
        
        try:
            ts = int(ts)
            # If ts is in seconds (< 1e11), convert to milliseconds
            if ts < 1e11:
                ts = ts * 1000
        except (ValueError, TypeError):
            logger.warning(json_msg({
                "event": "fill_skipped_unparseable_timestamp",
                "exchange_order_id": exchange_order_id,
                "ts": ts,
            }))
            return None
        
        # 7. Extract market_ticker
        market_ticker = f.get('market_ticker') or f.get('market') or f.get('ticker')
        if not market_ticker:
            logger.warning(json_msg({
                "event": "fill_skipped_missing_market",
                "exchange_order_id": exchange_order_id,
            }))
            return None
        
        # 8. Build normalized fill dict
        return {
            'fill_id': fill_id,
            'ts': ts,
            'market_ticker': market_ticker,
            'exchange_order_id': exchange_order_id,
            'client_order_id': f.get('client_order_id') or f.get('clientOrderId') or f.get('client_id'),
            'side': side,  # 'yes' or 'no'
            'price_cents': price_cents,
            'size': size,
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
        """
        Fetch fills from Kalshi API and insert ONLY real fills into ClickHouse.
        
        Strict predicate: A fill must have ALL of:
        - order_id / exchange_order_id
        - ts / timestamp
        - count (contract count)
        - yes_price or no_price or price (for price_cents)
        - Computed price_cents in [1, 99]
        
        Logging at each stage:
        - n_api: Fills returned by API
        - n_raw: After defensive type/shape handling
        - n_real: After applying "real fill" predicate
        - n_valid: After _normalize_fill() validation
        - n_inserted: After ClickHouse insertion
        """
        
        # Stage 0: Call API
        fills_raw = []
        try:
            fills_raw = self.exec.get_fills(self._last_fills_ts) or []
        except Exception:
            logger.exception('get_fills failed')
            return []
        
        n_api = len(fills_raw) if isinstance(fills_raw, (list, tuple)) else 0
        
        # Stage 1: Defensive normalization (ensure list of dicts)
        if isinstance(fills_raw, dict):
            # Extract fills array from dict
            fills_raw = fills_raw.get("fills") or fills_raw.get("data") or fills_raw.get("items") or []
        
        if isinstance(fills_raw, str):
            logger.error("get_fills returned str; dropping: %r", fills_raw[:200])
            fills_raw = []
        elif not isinstance(fills_raw, list):
            logger.error("get_fills returned %s; expected list", type(fills_raw))
            fills_raw = []
        
        # Ensure all items are dicts
        fills_raw = [f for f in fills_raw if isinstance(f, dict)]
        n_raw = len(fills_raw)
        
        if n_api == 0:
            return []
        
        # Stage 2: Fill source sanity check
        if fills_raw:
            first_item = fills_raw[0]
            logger.info(json_msg({
                "event": "fill_source_sanity",
                "n_api": n_api,
                "n_raw": n_raw,
                "item_keys": list(first_item.keys()),
                "has_fill_id": "fill_id" in first_item or "trade_id" in first_item,
                "has_order_id": "order_id" in first_item or "orderId" in first_item,
                "has_count": "count" in first_item,
                "has_price_fields": any(k in first_item for k in ["price", "yes_price", "no_price"]),
                "has_ts": "ts" in first_item or "timestamp" in first_item,
            }))
            logger.info(json_msg({"event": "raw_fill_sample", "fill": first_item}))
        
        # Stage 3: Apply "real fill" predicate
        # CRITICAL: Only process fills that have the essential fields of a REAL fill
        fills_real = []
        for f in fills_raw:
            # Must have order_id
            if not (f.get('order_id') or f.get('orderId') or f.get('id')):
                logger.debug("Fill dropped: missing order_id")
                continue
            
            # Must have timestamp
            if not (f.get('ts') or f.get('timestamp') or f.get('time')):
                logger.debug("Fill dropped: missing timestamp")
                continue
            
            # Must have count (number of contracts)
            if not f.get('count') and f.get('count') != 0:
                logger.debug("Fill dropped: missing count")
                continue
            
            # Must have price information (from side-specific or float price)
            has_price = any(k in f for k in ['price', 'yes_price', 'no_price'])
            if not has_price:
                logger.debug("Fill dropped: missing price fields")
                continue
            
            # Looks like a real fill
            fills_real.append(f)
        
        n_real = len(fills_real)
        
        # Log the predicate filter result
        if n_api != n_real:
            logger.warning(json_msg({
                "event": "fill_predicate_filtered",
                "n_api": n_api,
                "n_raw": n_raw,
                "n_real": n_real,
                "dropped_by_predicate": n_raw - n_real,
            }))
        
        if not fills_real:
            logger.info(json_msg({
                "event": "no_real_fills",
                "n_api": n_api,
                "n_raw": n_raw,
            }))
            return []
        
        # Stage 4: Normalize and validate
        norm_fills = [self._normalize_fill(f) for f in fills_real]
        norm_fills = [f for f in norm_fills if f is not None]
        n_valid = len(norm_fills)
        
        # Log normalization results
        if n_real != n_valid:
            logger.info(json_msg({
                "event": "fill_normalization_filtered",
                "n_real": n_real,
                "n_valid": n_valid,
                "dropped_by_validation": n_real - n_valid,
            }))
        
        if not norm_fills:
            return []
        
        # Get engine metadata for fills
        engine_instance_id = getattr(self.engine, 'state', None) and getattr(self.engine.state, 'instance_id', '') or ''
        engine_version = getattr(self.engine, 'state', None) and getattr(self.engine.state, 'version', 'dev') or 'dev'
        
        # Stage 5: Final strict validation gates before insert
        rows = []
        dropped_count = 0
        first_invalid_sample = None
        
        for f in norm_fills:
            # Gate 1: exchange_order_id non-empty
            if not f.get('exchange_order_id') or not str(f['exchange_order_id']).strip():
                dropped_count += 1
                if not first_invalid_sample:
                    first_invalid_sample = {'reason': 'empty_order_id', 'fill': f}
                continue
            
            # Gate 2: fill_id non-empty (or compute it)
            fill_id = f.get('fill_id')
            if not fill_id:
                # Compute synthetic fill_id as hash
                import hashlib
                fill_hash = hashlib.md5(f"{f['exchange_order_id']}:{f['side']}:{f['price_cents']}:{f['size']}:{f['ts']}".encode()).hexdigest()
                fill_id = fill_hash
            
            # Gate 3: size > 0
            if f.get('size', 0) <= 0:
                dropped_count += 1
                if not first_invalid_sample:
                    first_invalid_sample = {'reason': 'zero_size', 'fill': f}
                continue
            
            # Gate 4: price_cents in [1, 99]
            price_cents = f.get('price_cents', 0)
            if price_cents < 1 or price_cents > 99:
                dropped_count += 1
                if not first_invalid_sample:
                    first_invalid_sample = {'reason': 'invalid_price', 'price_cents': price_cents, 'fill': f}
                continue
            
            # Gate 5: market_ticker non-empty
            if not f.get('market_ticker') or not str(f['market_ticker']).strip():
                dropped_count += 1
                if not first_invalid_sample:
                    first_invalid_sample = {'reason': 'empty_market', 'fill': f}
                continue
            
            # Gate 6: side in {yes, no}
            if f.get('side') not in ('yes', 'no'):
                dropped_count += 1
                if not first_invalid_sample:
                    first_invalid_sample = {'reason': 'invalid_side', 'side': f.get('side'), 'fill': f}
                continue
            
            # All gates passed, build row for insert
            decision_id = None
            if self.engine:
                exch_id = f.get('exchange_order_id')
                if exch_id and hasattr(self.engine, 'state') and hasattr(self.engine.state, 'order_by_exchange_id'):
                    if exch_id in self.engine.state.order_by_exchange_id:
                        order_ref = self.engine.state.order_by_exchange_id[exch_id]
                        decision_id = order_ref.decision_id
            
            # CRITICAL FIX: Use the fill's actual timestamp, not now()
            # normalized fill has ts in milliseconds; convert to DateTime string
            from datetime import datetime, timezone
            fill_ts_ms = f.get('ts', 0)
            if fill_ts_ms > 0:
                fill_ts_sec = fill_ts_ms / 1000.0 if fill_ts_ms > 1e11 else fill_ts_ms
                try:
                    fill_dt = datetime.fromtimestamp(fill_ts_sec, tz=timezone.utc)
                    ts_str = fill_dt.strftime('%Y-%m-%d %H:%M:%S')
                except:
                    ts_str = time.strftime('%Y-%m-%d %H:%M:%S')
            else:
                ts_str = time.strftime('%Y-%m-%d %H:%M:%S')
            
            rows.append({
                'ts': ts_str,
                'engine_instance_id': engine_instance_id,
                'engine_version': engine_version,
                'market_ticker': f['market_ticker'],
                'exchange_order_id': f['exchange_order_id'],
                'client_order_id': f.get('client_order_id'),
                'side': f['side'],
                'price_cents': f['price_cents'],
                'size': f['size'],
                'decision_id': decision_id,
                'fill_id': fill_id,
                'raw_json': str(f['raw']),
            })
        
        n_inserted = len(rows)
        
        # Log insertion stats
        if dropped_count > 0:
            logger.warning(json_msg({
                "event": "fills_dropped_invalid_gates",
                "count": dropped_count,
                "total_after_validation": len(norm_fills),
                "first_invalid": first_invalid_sample,
            }))
        
        # Final comprehensive log
        # Include timestamp verification to confirm we're using fill's ts, not now()
        first_fill_ts_info = {}
        if norm_fills:
            first_fill = norm_fills[0]
            fill_ts_ms = first_fill.get('ts', 0)
            if fill_ts_ms > 0:
                from datetime import datetime, timezone
                fill_ts_sec = fill_ts_ms / 1000.0 if fill_ts_ms > 1e11 else fill_ts_ms
                try:
                    fill_dt = datetime.fromtimestamp(fill_ts_sec, tz=timezone.utc)
                    first_fill_ts_info = {
                        "first_fill_ts_iso": fill_dt.isoformat(),
                        "now_iso": datetime.now(timezone.utc).isoformat(),
                        "ts_age_sec": (datetime.now(timezone.utc) - fill_dt).total_seconds(),
                    }
                except:
                    pass
        
        logger.info(json_msg({
            "event": "fill_pipeline_stats",
            "n_api": n_api,
            "n_raw": n_raw,
            "n_real": n_real,
            "n_valid": n_valid,
            "n_inserted": n_inserted,
            "predicate_pass_rate": f"{100.0 * n_real / max(n_api, 1):.1f}%",
            "validation_pass_rate": f"{100.0 * n_valid / max(n_real, 1):.1f}%",
            "gate_pass_rate": f"{100.0 * n_inserted / max(n_valid, 1):.1f}%",
            **first_fill_ts_info,  # Include timestamp verification
        }))
        
        # Write to ClickHouse
        if self.ch and rows:
            try:
                # CRITICAL: Idempotency filter - don't re-insert fills that already exist
                # Query ClickHouse for fill_ids we already have to avoid duplicates
                existing_fill_ids = set()
                if rows:
                    incoming_fill_ids = [r['fill_id'] for r in rows if r.get('fill_id')]
                    if incoming_fill_ids:
                        try:
                            # Chunk the query to avoid URL length issues (max 500 at a time)
                            for i in range(0, len(incoming_fill_ids), 500):
                                chunk = incoming_fill_ids[i:i+500]
                                id_list = ', '.join([f"'{fid}'" for fid in chunk])
                                result = self.ch._exec_and_read(
                                    f"SELECT DISTINCT fill_id FROM kalshi.mm_fills WHERE fill_id IN ({id_list})"
                                )
                                if result:
                                    existing_fill_ids.update(line.strip() for line in result.strip().split('\n') if line.strip())
                        except Exception as e:
                            logger.warning(json_msg({
                                "event": "idempotency_check_failed",
                                "error": str(e),
                                "incoming_count": len(incoming_fill_ids),
                            }))
                            # If idempotency check fails, still insert (better to insert duplicate than drop valid fill)
                
                # Filter to only new fills
                new_rows = [r for r in rows if r.get('fill_id') not in existing_fill_ids]
                duplicates = len(rows) - len(new_rows)
                
                if duplicates > 0:
                    logger.info(json_msg({
                        "event": "fills_deduped",
                        "total_rows": len(rows),
                        "new_rows": len(new_rows),
                        "duplicates_filtered": duplicates,
                    }))
                
                # Insert only new rows
                if new_rows:
                    self.ch.insert('mm_fills', new_rows)
                    logger.info(json_msg({
                        "event": "fills_ingested",
                        "count": len(new_rows),
                        "with_decision_id": sum(1 for r in new_rows if r.get('decision_id')),
                        "with_side": sum(1 for r in new_rows if r.get('side')),
                    }))
                else:
                    logger.info(json_msg({
                        "event": "fills_all_duplicates",
                        "count": len(rows),
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
