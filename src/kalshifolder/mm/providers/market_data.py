import requests
import logging
from typing import Dict, List

logger = logging.getLogger(__name__)


class ClickHouseMarketDataProvider:
    def __init__(self, ch_url: str, user: str = 'default', db: str = 'default', timeout: float = 5.0):
        self.ch_url = ch_url.rstrip('/')
        self.user = user
        self.db = db
        self.timeout = timeout

    def _query(self, sql: str):
        try:
            r = requests.post(self.ch_url, params={'user': self.user, 'database': self.db}, data=sql.encode('utf-8'), timeout=self.timeout)
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
        sql = (
            "SELECT"
            " market_ticker,"
            " max(ts) AS ts,"
            " maxIf(price, side = 'yes') AS yes_bid_px,"
            " argMaxIf(size, price, side = 'yes') AS yes_bid_sz,"
            " maxIf(price, side = 'no') AS no_bid_px,"
            " argMaxIf(size, price, side = 'no') AS no_bid_sz"
            f" FROM kalshi.latest_levels WHERE market_ticker IN ({markets_list})"
            " GROUP BY market_ticker"
            " FORMAT JSONEachRow"
        )

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
                obj = requests.utils.json.loads(line)
            except Exception:
                logger.exception('Failed to parse CH JSON line')
                continue
            mt = obj.get('market_ticker')
            if not mt:
                continue
            ts = obj.get('ts')
            # convert ts to ms: ClickHouse returns ISO timestamp string or numeric; try parse
            ts_ms = None
            try:
                # If ts is in ISO format, use datetime
                from datetime import datetime
                if isinstance(ts, str):
                    # example: "2025-12-27 12:34:56.123456"
                    dt = datetime.fromisoformat(ts)
                    ts_ms = int(dt.timestamp() * 1000)
                else:
                    ts_ms = int(ts)
            except Exception:
                ts_ms = None

            yes_px = obj.get('yes_bid_px')
            yes_sz = obj.get('yes_bid_sz')
            no_px = obj.get('no_bid_px')
            no_sz = obj.get('no_bid_sz')

            # Build result; convert cents to dollars
            results[mt] = {
                'bb_px': (yes_px / 100.0) if yes_px else None,
                'bb_sz': yes_sz or 0,
                'ba_px': ((100 - no_px) / 100.0) if no_px else None,
                'ba_sz': no_sz or 0,
                'ts_ms': ts_ms,
            }

        return results

    def get_best_bid_ask(self, market_ticker: str):
        res = self.get_batch_best_bid_ask([market_ticker])
        return res.get(market_ticker)
