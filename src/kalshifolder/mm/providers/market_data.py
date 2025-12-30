import requests
import logging
from typing import Dict, List
import json

logger = logging.getLogger(__name__)


class ClickHouseMarketDataProvider:
    def __init__(self, ch_url: str, user: str = 'default', pwd: str = '', db: str = 'default', timeout: float = 5.0):
        self.ch_url = ch_url.rstrip('/')
        self.user = user
        self.pwd = pwd
        self.db = db
        self.timeout = timeout

    def _query(self, sql: str):
        try:
            params = {'database': self.db}
            if self.user:
                params['user'] = self.user
            if self.pwd:
                params['password'] = self.pwd
            r = requests.post(self.ch_url, params=params, data=sql.encode('utf-8'), timeout=self.timeout)
            r.raise_for_status()
            return r.text
        except Exception:
            logger.exception('CH query failed')
            raise

    def get_batch_best_bid_ask(self, markets: List[str]) -> Dict[str, dict]:
        """
        Query kalshi.latest_levels in a single batch and return per-ticker top-of-book.

        Returns dict keyed by market_ticker with fields: bb_px, bb_sz, ba_px, ba_sz, ts_ms
        Prices are in dollars (float).
        """
        if not markets:
            return {}
        markets_list = ','.join([f"'{m}'" for m in markets])
        # Known-good merge-independent SQL: dedupe per (market_ticker, side, price) by ingest_ts,
        # then compute best bid/ask per side. This pattern avoids depending on background merges.
        sql = f"""
WITH lv AS (
    SELECT
    market_ticker,
    side,
    price,
    argMax(size, ingest_ts) AS size,
    argMax(ingest_ts, ingest_ts) AS ingest_ts,
    argMax(ts, ingest_ts) AS exchange_ts
    FROM kalshi.latest_levels
    WHERE market_ticker IN ({markets_list})
    GROUP BY market_ticker, side, price
)
SELECT
    market_ticker,
    max(ingest_ts) AS ingest_ts,
    max(exchange_ts) AS exchange_ts,

    maxIf(price, side='yes' AND size > 0) AS yes_bid_px,
    argMaxIf(size, price, side='yes' AND size > 0) AS yes_bid_sz,
    minIf(price, side='yes' AND size > 0) AS yes_ask_px,
    argMinIf(size, price, side='yes' AND size > 0) AS yes_ask_sz,

    maxIf(price, side='no' AND size > 0) AS no_bid_px,
    argMaxIf(size, price, side='no' AND size > 0) AS no_bid_sz,
    minIf(price, side='no' AND size > 0) AS no_ask_px,
    argMinIf(size, price, side='no' AND size > 0) AS no_ask_sz
FROM lv
GROUP BY market_ticker
FORMAT JSONEachRow
"""

        try:
            txt = self._query(sql)
        except Exception:
            # bubble up so caller can mark markets stale / API-unhealthy
            raise

        results: Dict[str, dict] = {}
        # parse JSONEachRow lines
        for line in txt.splitlines():
            if not line.strip():
                continue
            try:
                obj = json.loads(line)
            except Exception:
                logger.exception('Failed to parse CH JSON line')
                continue
            mt = obj.get('market_ticker')
            if not mt:
                continue
            # normalize ingest_ts and exchange_ts to ms
            ingest = obj.get('ingest_ts') or obj.get('ingest_ts_ms') or obj.get('ingest_ts')
            exchange_ts = obj.get('exchange_ts') or obj.get('exchange_ts_ms') or obj.get('exchange_ts')
            def _to_ms(val):
                if val is None:
                    return None
                try:
                    if isinstance(val, str):
                        from datetime import datetime
                        dt = datetime.fromisoformat(val)
                        return int(dt.timestamp() * 1000)
                    return int(val)
                except Exception:
                    return None

            ingest_ms = _to_ms(ingest)
            exchange_ms = _to_ms(exchange_ts)

            # support both legacy and new field names
            yes_bid_px = obj.get('yes_bb_px') or obj.get('yes_bid_px') or obj.get('bb_px')
            yes_bid_sz = obj.get('yes_bb_sz') or obj.get('yes_bid_sz') or obj.get('bb_sz')
            yes_ask_px = obj.get('yes_ba_px') or obj.get('yes_ask_px') or obj.get('ba_px')
            yes_ask_sz = obj.get('yes_ba_sz') or obj.get('yes_ask_sz') or obj.get('ba_sz')
            no_bid_px = obj.get('no_bb_px') or obj.get('no_bid_px')
            no_bid_sz = obj.get('no_bb_sz') or obj.get('no_bid_sz')
            no_ask_px = obj.get('no_ba_px') or obj.get('no_ask_px')
            no_ask_sz = obj.get('no_ba_sz') or obj.get('no_ask_sz')

            def _px_to_dollars(px):
                if px is None:
                    return None
                try:
                    return px / 100.0
                except Exception:
                    return None

            results[mt] = {
                'yes_bb_px': _px_to_dollars(yes_bid_px),
                'yes_bb_sz': yes_bid_sz or 0,
                'yes_ba_px': _px_to_dollars(yes_ask_px),
                'yes_ba_sz': yes_ask_sz or 0,
                'no_bb_px': _px_to_dollars(no_bid_px),
                'no_bb_sz': no_bid_sz or 0,
                'no_ba_px': _px_to_dollars(no_ask_px),
                'no_ba_sz': no_ask_sz or 0,
                'ingest_ts_ms': ingest_ms,
                'exchange_ts_ms': exchange_ms,
            }

        return results

    def get_best_bid_ask(self, market_ticker: str):
        res = self.get_batch_best_bid_ask([market_ticker])
        return res.get(market_ticker)
