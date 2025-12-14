#!/usr/bin/env python3
"""
ClickHouse QA for KalshiMonorepo

Focus:
- Per-market continuity (gaps)
- Clock weirdness (ts vs ingest_ts)
- Event rates + silent ingestion death
- SID collision / identity integrity checks
- Seq sanity (out-of-order, big jumps)
- Snapshot/delta event ratios (event-level, not row-level)
- Basic latest_levels sanity (spread, crossed markets)
"""

import os
import sys
import argparse
from datetime import datetime, timezone, timedelta
from typing import Optional, List, Dict, Any

try:
    import clickhouse_connect  # pip install clickhouse-connect
except Exception:
    clickhouse_connect = None


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _fmt_rows(rows: List[tuple], max_rows: int = 20) -> str:
    if not rows:
        return "(none)"
    out = []
    for i, r in enumerate(rows[:max_rows], start=1):
        out.append(f"{i:>2}. " + " | ".join(str(x) for x in r))
    if len(rows) > max_rows:
        out.append(f"... ({len(rows)-max_rows} more)")
    return "\n".join(out)


def ch_client_from_env():
    """
    Supports either:
      CLICKHOUSE_URL=http://clickhouse:8123
    or:
      CLICKHOUSE_HOST=clickhouse CLICKHOUSE_PORT=8123
    """
    if clickhouse_connect is None:
        raise RuntimeError("Missing dependency clickhouse-connect. Install: pip install clickhouse-connect")

    url = (os.getenv("CLICKHOUSE_URL") or "").strip()
    user = (os.getenv("CLICKHOUSE_USER") or "default").strip()
    password = (os.getenv("CLICKHOUSE_PASSWORD") or "").strip()
    database = (os.getenv("CLICKHOUSE_DATABASE") or "kalshi").strip()

    if url.startswith("http://") or url.startswith("https://"):
        # clickhouse-connect wants host/port; parse simply
        # expected format: http://host:8123
        u = url.replace("http://", "").replace("https://", "")
        host, port = u.split(":")
        port = int(port)
    else:
        host = (os.getenv("CLICKHOUSE_HOST") or "localhost").strip()
        port = int(os.getenv("CLICKHOUSE_PORT") or "8123")

    return clickhouse_connect.get_client(
        host=host,
        port=port,
        username=user,
        password=password if password else None,
        database=database,
    )


def q(client, sql: str, params: Optional[Dict[str, Any]] = None) -> List[tuple]:
    # clickhouse-connect: client.query(sql, parameters=...) returns a result obj
    res = client.query(sql, parameters=params or {})
    return res.result_rows


# -------------------------
# QA CHECKS (SQL)
# -------------------------

SQL_GLOBAL_ROWS_PER_DAY = """
SELECT
  toDate(ts) AS d,
  count() AS rows
FROM {db}.orderbook_events
WHERE ts >= now() - INTERVAL {days:Int32} DAY
GROUP BY d
ORDER BY d DESC
"""

SQL_PER_TICKER_ROWS_PER_DAY = """
SELECT
  market_ticker,
  toDate(ts) AS d,
  count() AS rows
FROM {db}.orderbook_events
WHERE ts >= now() - INTERVAL {days:Int32} DAY
GROUP BY market_ticker, d
ORDER BY d DESC, rows DESC
"""

SQL_LATEST_PER_TICKER = """
SELECT
  market_ticker,
  max(ts) AS last_ts,
  max(ingest_ts) AS last_ingest_ts,
  count() AS rows
FROM {db}.orderbook_events
WHERE ts >= now() - INTERVAL {hours:Int32} HOUR
GROUP BY market_ticker
ORDER BY last_ts DESC
"""

# True event-level ratio: uniq (sid,seq)
SQL_SNAPSHOT_DELTA_EVENT_RATIO = """
SELECT
  market_ticker,
  uniqExactIf((sid, seq), type='snapshot') AS snapshot_events,
  uniqExactIf((sid, seq), type='delta')    AS delta_events,
  round(snapshot_events / greatest(delta_events, 1), 3) AS snaps_per_delta_event,
  max(ts) AS last_ts
FROM {db}.orderbook_events
WHERE ts >= now() - INTERVAL {hours:Int32} HOUR
GROUP BY market_ticker
ORDER BY snaps_per_delta_event DESC
"""

# SID collision risk: sid used by multiple tickers (historically your #1 problem)
SQL_SID_MULTI_TICKER = """
SELECT
  sid,
  countDistinct(market_ticker) AS uniq_tickers,
  any(market_ticker) AS example_ticker,
  count() AS rows
FROM {db}.orderbook_events
WHERE ts >= now() - INTERVAL {hours:Int32} HOUR
GROUP BY sid
HAVING uniq_tickers > 1
ORDER BY uniq_tickers DESC, rows DESC
LIMIT 50
"""

# Seq sanity: out-of-order seq within ticker (best-effort; relies on ts ordering)
SQL_SEQ_OUT_OF_ORDER = """
WITH ev AS (
  SELECT
    market_ticker,
    ts,
    sid,
    seq,
    lagInFrame(seq) OVER (PARTITION BY market_ticker ORDER BY ts, ingest_ts) AS prev_seq
  FROM {db}.orderbook_events
  WHERE ts >= now() - INTERVAL {hours:Int32} HOUR
  GROUP BY market_ticker, ts, sid, seq, ingest_ts
)
SELECT
  market_ticker,
  countIf(prev_seq IS NOT NULL AND seq < prev_seq) AS out_of_order_count,
  count() AS events_count,
  round(out_of_order_count / greatest(events_count, 1), 6) AS frac_out_of_order
FROM ev
GROUP BY market_ticker
HAVING out_of_order_count > 0
ORDER BY out_of_order_count DESC
LIMIT 50
"""

# Gaps per ticker: max time between consecutive event timestamps
SQL_MAX_GAP_SECONDS_PER_TICKER = """
WITH ev AS (
  SELECT
    market_ticker,
    ts,
    lagInFrame(ts) OVER (PARTITION BY market_ticker ORDER BY ts, ingest_ts) AS prev_ts
  FROM {db}.orderbook_events
  WHERE ts >= now() - INTERVAL {hours:Int32} HOUR
  GROUP BY market_ticker, ts, ingest_ts
)
SELECT
  market_ticker,
  max(ts - prev_ts) AS max_gap_seconds,
  quantileExact(0.99)(ts - prev_ts) AS p99_gap_seconds,
  count() AS points
FROM ev
WHERE prev_ts IS NOT NULL
GROUP BY market_ticker
ORDER BY max_gap_seconds DESC
LIMIT 50
"""

# Clock weirdness: ingest_ts should not be wildly earlier than ts, or wildly late
SQL_CLOCK_SKEW = """
SELECT
  market_ticker,
  quantileExact(0.01)(dateDiff('second', ts, ingest_ts)) AS p01_ingest_minus_ts_s,
  quantileExact(0.50)(dateDiff('second', ts, ingest_ts)) AS p50_ingest_minus_ts_s,
  quantileExact(0.99)(dateDiff('second', ts, ingest_ts)) AS p99_ingest_minus_ts_s,
  max(dateDiff('second', ts, ingest_ts)) AS max_ingest_minus_ts_s,
  min(dateDiff('second', ts, ingest_ts)) AS min_ingest_minus_ts_s
FROM {db}.orderbook_events
WHERE ts >= now() - INTERVAL {hours:Int32} HOUR
GROUP BY market_ticker
ORDER BY max_ingest_minus_ts_s DESC
LIMIT 50
"""

# latest_levels sanity: crossed markets (best bid >= best ask) is a major red flag
# Adjust price direction if your contract semantics differ; for yes/no it still should not be crossed within same side book.
SQL_LATEST_LEVELS_CROSSED = """
WITH best AS (
  SELECT
    market_ticker,
    maxIf(price, side='yes' AND size > 0) AS best_yes_bid, -- NOTE: assumes higher price = better bid
    minIf(price, side='yes' AND size > 0) AS best_yes_ask, -- if you store asks separately you may need different logic
    maxIf(ingest_ts, 1) AS last_ingest
  FROM {db}.latest_levels
  GROUP BY market_ticker
)
SELECT
  market_ticker,
  best_yes_bid,
  best_yes_ask,
  last_ingest
FROM best
WHERE best_yes_bid > 0 AND best_yes_ask > 0 AND best_yes_bid > best_yes_ask
ORDER BY last_ingest DESC
LIMIT 50
"""

# Negative qty / impossible values
SQL_NEGATIVE_QTY = """
SELECT
  market_ticker,
  type,
  count() AS n
FROM {db}.orderbook_events
WHERE ts >= now() - INTERVAL {hours:Int32} HOUR
  AND (
    (type='snapshot' AND qty < 0) OR
    (type='delta' AND qty = 0) OR
    (price < 0 OR price > 100)
  )
GROUP BY market_ticker, type
ORDER BY n DESC
LIMIT 50
"""


def run_report(client, db: str, hours: int, days: int):
    print("\n=== QA: GLOBAL ROWS PER DAY ===")
    rows = q(client, SQL_GLOBAL_ROWS_PER_DAY.format(db=db), {"days": days})
    print(_fmt_rows(rows, 30))

    print("\n=== QA: PER-TICKER LAST SEEN (recent) ===")
    rows = q(client, SQL_LATEST_PER_TICKER.format(db=db), {"hours": hours})
    print(_fmt_rows(rows, 30))

    print("\n=== QA: SNAPSHOT vs DELTA EVENT RATIO (event-level) ===")
    rows = q(client, SQL_SNAPSHOT_DELTA_EVENT_RATIO.format(db=db), {"hours": hours})
    print(_fmt_rows(rows, 30))
    print("Interpretation: high snaps_per_delta_event usually means frequent resnapshots or low delta activity.")

    print("\n=== QA: SID USED BY MULTIPLE TICKERS (historical #1 bug) ===")
    rows = q(client, SQL_SID_MULTI_TICKER.format(db=db), {"hours": hours})
    print(_fmt_rows(rows, 50))
    if rows:
        print("NOTE: This can be OK only if your streamer is NOT keying state by sid. If it is, it's corrupting data.")

    print("\n=== QA: MAX GAP SECONDS PER TICKER ===")
    rows = q(client, SQL_MAX_GAP_SECONDS_PER_TICKER.format(db=db), {"hours": hours})
    print(_fmt_rows(rows, 30))

    print("\n=== QA: SEQ OUT-OF-ORDER (best-effort) ===")
    rows = q(client, SQL_SEQ_OUT_OF_ORDER.format(db=db), {"hours": hours})
    print(_fmt_rows(rows, 30))

    print("\n=== QA: CLOCK SKEW (ingest_ts - ts) ===")
    rows = q(client, SQL_CLOCK_SKEW.format(db=db), {"hours": hours})
    print(_fmt_rows(rows, 30))
    print("Interpretation: big negative means ts > ingest_ts (bad clocks/parsing). Big positive means ingest lag/backpressure.")

    print("\n=== QA: NEGATIVE/IMPOSSIBLE VALUES ===")
    rows = q(client, SQL_NEGATIVE_QTY.format(db=db), {"hours": hours})
    print(_fmt_rows(rows, 30))

    print("\n=== QA: latest_levels CROSSED MARKET (heuristic) ===")
    # This query may need adjustment depending on how you represent bid/ask on yes/no
    try:
        rows = q(client, SQL_LATEST_LEVELS_CROSSED.format(db=db), {})
        print(_fmt_rows(rows, 30))
    except Exception as e:
        print(f"(skipped: latest_levels check error: {e})")

    print("\n=== DONE ===\n")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=os.getenv("CLICKHOUSE_DATABASE", "kalshi"))
    ap.add_argument("--hours", type=int, default=24, help="Lookback window for most checks")
    ap.add_argument("--days", type=int, default=14, help="Lookback window for global/day checks")
    args = ap.parse_args()

    client = ch_client_from_env()
    run_report(client, db=args.db, hours=args.hours, days=args.days)


if __name__ == "__main__":
    main()
