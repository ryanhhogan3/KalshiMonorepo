import requests
import logging
from typing import Dict, List
import json

logger = logging.getLogger(__name__)


class ClickHouseMarketDataProvider:
    def __init__(
        self,
        ch_url: str,
        user: str = "default",
        pwd: str = "",
        db: str = "default",
        timeout: float = 5.0,
        latest_table: str = "kalshi.latest_levels_v2",
    ):
        self.ch_url = ch_url.rstrip("/")
        self.user = user
        self.pwd = pwd
        self.db = db
        self.timeout = timeout
        self.latest_table = latest_table

    def _fq_table(self) -> str:
        """
        Fully qualify latest table safely.
        Accepts 'latest_levels_v2' or 'kalshi.latest_levels_v2'.
        """
        t = (self.latest_table or "").strip()
        if "." in t:
            return t
        return f"{self.db}.{t}"

    def _query(self, sql: str):
        params = {"database": self.db}
        if self.user:
            params["user"] = self.user
        if self.pwd:
            params["password"] = self.pwd

        r = None
        try:
            r = requests.post(
                self.ch_url,
                params=params,
                data=sql.encode("utf-8"),
                timeout=self.timeout,
            )
            if r.status_code >= 400:
                # Print ClickHouse error body + SQL (super important for debugging 500s)
                body = (r.text or "")[:4000]
                logger.error("---- CH HTTP ERROR ---- status=%s body=%s sql=%s", r.status_code, body, sql)
            r.raise_for_status()
            return r.text
        except Exception:
            # If requests blew up before we got a response, still log the SQL
            logger.exception("CH query failed; sql=%s", sql)
            raise

    def get_batch_best_bid_ask(self, markets: List[str]) -> Dict[str, dict]:
        if not markets:
            return {}

        markets_list = ",".join([f"'{m}'" for m in markets])
        lt = self._fq_table()

        sql = f"""
WITH
dedup AS (
  SELECT
    market_ticker,
    side,
    price,
    argMax(size, ingest_ts) AS size
  FROM {lt}
  WHERE market_ticker IN ({markets_list})
  GROUP BY market_ticker, side, price
),
bbo AS (
  SELECT
    market_ticker,

    maxIf(price, side='yes' AND size > 0) AS yes_bid_px,
    argMaxIf(size, price, side='yes' AND size > 0) AS yes_bid_sz,
    minIf(price, side='yes' AND size > 0) AS yes_ask_px,
    argMinIf(size, price, side='yes' AND size > 0) AS yes_ask_sz,

    maxIf(price, side='no' AND size > 0) AS no_bid_px,
    argMaxIf(size, price, side='no' AND size > 0) AS no_bid_sz,
    minIf(price, side='no' AND size > 0) AS no_ask_px,
    argMinIf(size, price, side='no' AND size > 0) AS no_ask_sz
  FROM dedup
  GROUP BY market_ticker
),
meta AS (
  SELECT
    market_ticker,
    max(ingest_ts) AS ingest_ts,
    max(ts) AS exchange_ts
  FROM {lt}
  WHERE market_ticker IN ({markets_list})
  GROUP BY market_ticker
)
SELECT
  bbo.market_ticker,
  meta.ingest_ts,
  meta.exchange_ts,
  bbo.yes_bid_px, bbo.yes_bid_sz, bbo.yes_ask_px, bbo.yes_ask_sz,
  bbo.no_bid_px,  bbo.no_bid_sz,  bbo.no_ask_px,  bbo.no_ask_sz
FROM bbo
INNER JOIN meta USING (market_ticker)
FORMAT JSONEachRow
"""

        txt = self._query(sql)

        results: Dict[str, dict] = {}
        for line in txt.splitlines():
            if not line.strip():
                continue
            try:
                obj = json.loads(line)
            except Exception:
                logger.exception("Failed to parse CH JSON line")
                continue

            mt = obj.get("market_ticker")
            if not mt:
                continue

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

            def _px_to_dollars(px):
                if px is None:
                    return None
                try:
                    return px / 100.0
                except Exception:
                    return None

            results[mt] = {
                "yes_bb_px": _px_to_dollars(obj.get("yes_bid_px")),
                "yes_bb_sz": obj.get("yes_bid_sz") or 0,
                "yes_ba_px": _px_to_dollars(obj.get("yes_ask_px")),
                "yes_ba_sz": obj.get("yes_ask_sz") or 0,
                "no_bb_px": _px_to_dollars(obj.get("no_bid_px")),
                "no_bb_sz": obj.get("no_bid_sz") or 0,
                "no_ba_px": _px_to_dollars(obj.get("no_ask_px")),
                "no_ba_sz": obj.get("no_ask_sz") or 0,
                "ingest_ts_ms": _to_ms(obj.get("ingest_ts")),
                "exchange_ts_ms": _to_ms(obj.get("exchange_ts")),
            }

        return results
