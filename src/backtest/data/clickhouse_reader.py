"""ClickHouse reader tailored to `orderbook_events` schema.

This module implements:
- get_snapshot_before(market_ticker, t_start_ms) -> (snapshot_seq, snapshot_ts)
- fetch_window(market_ticker, snapshot_seq, snapshot_ts, t_end_ms) -> list of rows
- stream_events(...) generator that yields rows in deterministic order.
- optional write_cache to write a pandas DataFrame to parquet.

Schema expectations (orderbook_events table):
- market_ticker (string)
- type ("snapshot" | "delta")
- sid (int)  -- stream id or source id
- seq (int)  -- sequence number for snapshot grouping and ordering
- ts_ms (int) -- event timestamp in unix ms
- side ("yes"|"no" or "buy"|"sell")
- price (float)
- qty (numeric signed; >0 bids, <0 asks, 0 delete)
- ingest_ts (int) optional

Adapt SQL column names if your ClickHouse schema differs.
"""

from __future__ import annotations

import os
import json
import requests
from typing import Generator, Dict, Any, Optional, List
import pandas as pd
from requests.auth import HTTPBasicAuth


def _ch_url() -> str:
    return os.getenv("CH_URL", "http://localhost:8123")

def _ch_db() -> str:
    # match docker-compose: CLICKHOUSE_DATABASE=kalshi
    return os.getenv("CLICKHOUSE_DATABASE", os.getenv("CH_DATABASE", "kalshi"))

def _auth():
    user = os.getenv("CH_USER", os.getenv("CLICKHOUSE_USER", "default"))
    pwd  = os.getenv("CH_PASSWORD", os.getenv("CLICKHOUSE_PASSWORD", "default_password"))
    return HTTPBasicAuth(user, pwd)


def _query(sql: str) -> List[Dict[str, Any]]:
    url = _ch_url()
    user = os.getenv("CH_USER", "default")
    pwd  = os.getenv("CH_PASSWORD", "")
    sql = sql.strip() + " FORMAT JSONEachRow"
    resp = requests.post(
        url,
        data=sql.encode("utf-8"),
        timeout=60,
        auth=HTTPBasicAuth(user, pwd),
    )
    resp.raise_for_status()
    rows: List[Dict[str, Any]] = []
    for line in resp.iter_lines(decode_unicode=True):
        if not line:
            continue
        try:
            rows.append(json.loads(line))
        except Exception:
            # skip malformed
            continue
    return rows

def get_snapshot_before(market_ticker: str, t_start_unix_ms: int) -> Optional[Dict[str, Any]]:
    db = _ch_db()
    sql = (
    "SELECT "
    "  seq, "
    "  toUnixTimestamp64Milli(ts) AS ts_ms "
    "FROM kalshi.orderbook_events "
    f"WHERE market_ticker = '{market_ticker}' "
    "  AND type = 'snapshot' "
    f"  AND ts <= toDateTime64({t_start_unix_ms}/1000.0, 3) "
    "ORDER BY ts DESC, seq DESC "
    "LIMIT 1"
    )
    rows = _query(sql)
    return rows[0] if rows else None


def fetch_snapshot_rows(market_ticker: str, snapshot_seq: int) -> List[Dict[str, Any]]:
    """Fetch all rows that belong to the snapshot sequence (same seq)."""
    sql = (
        "SELECT "
        "  market_ticker, type, sid, seq, "
        "  toUnixTimestamp64Milli(ts) AS ts_ms, "
        "  ingest_ts, "
        "  side, "
        "  if(side='no', 100 - price, price) AS price, "
        "  if(side='no', -abs(qty), abs(qty)) AS qty "
        "FROM kalshi.orderbook_events "
        f"WHERE market_ticker = '{market_ticker}' "
        "  AND type = 'snapshot' "
        f"  AND seq = {snapshot_seq} "
        "ORDER BY ts ASC, seq ASC, sid ASC, price ASC"
    )
    return _query(sql)



def fetch_window(market_ticker: str, start_ts_ms: int, end_ts_ms: int) -> List[Dict[str, Any]]:
    db = _ch_db()
    sql = (
    "SELECT "
    "  market_ticker, type, sid, seq, "
    "  toUnixTimestamp64Milli(ts) AS ts_ms, "
    "  side, price, qty "
    "FROM kalshi.orderbook_events "
    f"WHERE market_ticker = '{market_ticker}' "
    f"  AND toUnixTimestamp64Milli(ts) >= {start_ts_ms} "
    f"  AND toUnixTimestamp64Milli(ts) <= {end_ts_ms} "
    "  AND ( (side='yes' AND qty>0) OR (side='no' AND qty<0) ) "
    "ORDER BY ts_ms ASC, seq ASC, sid ASC, side ASC, price ASC"
)

    return _query(sql)

def stream_events(market_ticker: str, start_ts_ms: int, end_ts_ms: int) -> Generator[Dict[str, Any], None, None]:
    """Generator over deterministic ordered rows between start and end (inclusive)."""
    for row in fetch_window(market_ticker, start_ts_ms, end_ts_ms):
        yield row


def write_cache(rows: List[Dict[str, Any]], path: str) -> None:
    """Write rows to parquet cache (rows is list of dicts).

    The function converts to pandas DataFrame and writes parquet.
    """
    df = pd.DataFrame(rows)
    df.to_parquet(path, index=False)

