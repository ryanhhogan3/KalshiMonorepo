#!/usr/bin/env python3
"""
ClickHouse QA for KalshiMonorepo (stdlib-only)

Goals:
- Verify identity integrity (market_id is real UUID, not nil; ticker<->market_id mapping stable)
- Verify ingestion continuity (last_ts per ticker, gaps)
- Verify sequencing sanity (best-effort out-of-order + big jumps)
- Verify snapshot/delta event-level ratios (uniq(sid,seq))
- Verify clock skew (ingest_ts - ts)
- Verify value sanity (price bounds, negative snapshot qty, etc.)

No external deps:
- Uses ClickHouse HTTP interface via urllib (works anywhere you can reach CLICKHOUSE_URL).
"""

import os
import sys
import json
import argparse
from datetime import datetime, timezone
from urllib.parse import urlparse, urlencode
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError
from typing import Any, Dict, List, Tuple, Optional


# ----------------------------
# Helpers
# ----------------------------

def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _fmt_rows(rows: List[Tuple[Any, ...]], max_rows: int = 30) -> str:
    if not rows:
        return "(none)"
    out = []
    for i, r in enumerate(rows[:max_rows], start=1):
        out.append(f"{i:>2}. " + " | ".join(str(x) for x in r))
    if len(rows) > max_rows:
        out.append(f"... ({len(rows) - max_rows} more)")
    return "\n".join(out)


class CHHTTPClient:
    """
    Minimal ClickHouse HTTP client.
    Expects CLICKHOUSE_URL like:
      - http://clickhouse:8123
      - http://localhost:8123
    Auth via CLICKHOUSE_USER / CLICKHOUSE_PASSWORD if set.
    """
    def __init__(self, base_url: str, database: str, user: str, password: str):
        self.base_url = base_url.rstrip("/")
        self.database = database
        self.user = user
        self.password = password

    def query(self, sql: str, timeout: int = 30) -> Tuple[List[str], List[Tuple[Any, ...]]]:
        # Always request JSON output so we can parse reliably
        sql = sql.strip().rstrip(";") + " FORMAT JSON"

        params = {"database": self.database}
        if self.user:
            params["user"] = self.user
        if self.password:
            params["password"] = self.password

        url = f"{self.base_url}/?{urlencode(params)}"
        req = Request(
            url,
            data=sql.encode("utf-8"),
            headers={"Content-Type": "text/plain; charset=utf-8"},
            method="POST",
        )

        try:
            with urlopen(req, timeout=timeout) as resp:
                body = resp.read().decode("utf-8", errors="replace")
        except HTTPError as e:
            detail = e.read().decode("utf-8", errors="replace") if hasattr(e, "read") else str(e)
            raise RuntimeError(f"ClickHouse HTTPError {e.code}: {detail}") from e
        except URLError as e:
            raise RuntimeError(f"ClickHouse URLError: {e}") from e

        try:
            j = json.loads(body)
        except Exception as e:
            raise RuntimeError(f"Failed to parse ClickHouse JSON response. Body head: {body[:2000]}") from e

        cols = [m["name"] for m in j.get("meta", [])]
        data = j.get("data", [])
        rows: List[Tuple[Any, ...]] = []
        for row in data:
            rows.append(tuple(row.get(c) for c in cols))
        return cols, rows


def ch_client_from_env() -> CHHTTPClient:
    url = (os.getenv("CLICKHOUSE_URL") or "").strip()
    if not url:
        # fallback to host/port style
        host = (os.getenv("CLICKHOUSE_HOST") or "localhost").strip()
        port = int(os.getenv("CLICKHOUSE_PORT") or "8123")
        url = f"http://{host}:{port}"

    # sanity
    p = urlparse(url)
    if p.scheme not in ("http", "https"):
        raise RuntimeError(f"CLICKHOUSE_URL must be http(s)://..., got: {url}")

    user = (os.getenv("CLICKHOUSE_USER") or "default").strip()
    password = (os.getenv("CLICKHOUSE_PASSWORD") or "").strip()
    db = (os.getenv("CLICKHOUSE_DATABASE") or "kalshi").strip()
    return CHHTTPClient(url, database=db, user=user, password=password)


def _hours(n: int) -> int:
    n = int(n)
    if n <= 0:
        raise ValueError("hours must be > 0")
    return n


def _days(n: int) -> int:
    n = int(n)
    if n <= 0:
        raise ValueError("days must be > 0")
    return n


# ----------------------------
# SQL (most important checks)
# ----------------------------

def sql_global_rows_per_day(db: str, days: int) -> str:
    return f"""
    SELECT toDate(ts) AS d, count() AS rows
    FROM {db}.orderbook_events
    WHERE ts >= now() - INTERVAL {int(days)} DAY
    GROUP BY d
    ORDER BY d DESC
    """


def sql_last_seen_per_ticker(db: str, hours: int) -> str:
    return f"""
    SELECT
      market_ticker,
      max(ts) AS last_ts,
      max(ingest_ts) AS last_ingest_ts,
      count() AS rows
    FROM {db}.orderbook_events
    WHERE ts >= now() - INTERVAL {int(hours)} HOUR
    GROUP BY market_ticker
    ORDER BY last_ts DESC
    """


def sql_snapshot_delta_event_ratio(db: str, hours: int) -> str:
    return f"""
    SELECT
      market_ticker,
      uniqExactIf((sid, seq), type='snapshot') AS snapshot_events,
      uniqExactIf((sid, seq), type='delta')    AS delta_events,
      round(snapshot_events / greatest(delta_events, 1), 3) AS snaps_per_delta_event,
      max(ts) AS last_ts
    FROM {db}.orderbook_events
    WHERE ts >= now() - INTERVAL {int(hours)} HOUR
    GROUP BY market_ticker
    ORDER BY snaps_per_delta_event DESC
    """


def sql_identity_nil_uuid(db: str, hours: int) -> str:
    return f"""
    SELECT
      countIf(market_id = toUUID('00000000-0000-0000-0000-000000000000')) AS nil_market_id_rows,
      count() AS rows,
      round(nil_market_id_rows / greatest(rows, 1), 8) AS frac_nil
    FROM {db}.orderbook_events
    WHERE ts >= now() - INTERVAL {int(hours)} HOUR
    """


def sql_ticker_to_market_id_stability(db: str, hours: int) -> str:
    # This is the real “identity integrity” check for your analytics.
    return f"""
    SELECT
      market_ticker,
      countDistinct(market_id) AS uniq_market_ids,
      any(market_id) AS example_market_id,
      count() AS rows
    FROM {db}.orderbook_events
    WHERE ts >= now() - INTERVAL {int(hours)} HOUR
    GROUP BY market_ticker
    HAVING uniq_market_ids > 1
    ORDER BY uniq_market_ids DESC, rows DESC
    LIMIT 50
    """


def sql_market_id_to_ticker_stability(db: str, hours: int) -> str:
    return f"""
    SELECT
      market_id,
      countDistinct(market_ticker) AS uniq_tickers,
      any(market_ticker) AS example_ticker,
      count() AS rows
    FROM {db}.orderbook_events
    WHERE ts >= now() - INTERVAL {int(hours)} HOUR
    GROUP BY market_id
    HAVING uniq_tickers > 1
    ORDER BY uniq_tickers DESC, rows DESC
    LIMIT 50
    """


def sql_sid_multi_ticker(db: str, hours: int) -> str:
    # This is NOT automatically “bad” anymore (because you key state by ticker),
    # but it’s a useful warning signal and confirms Kalshi bundling behavior.
    return f"""
    SELECT
      sid,
      countDistinct(market_ticker) AS uniq_tickers,
      any(market_ticker) AS example_ticker,
      count() AS rows
    FROM {db}.orderbook_events
    WHERE ts >= now() - INTERVAL {int(hours)} HOUR
    GROUP BY sid
    HAVING uniq_tickers > 1
    ORDER BY uniq_tickers DESC, rows DESC
    LIMIT 50
    """


def sql_max_gap_seconds_per_ticker(db: str, hours: int) -> str:
    return f"""
    WITH ev AS (
      SELECT
        market_ticker,
        ts,
        ingest_ts,
        lagInFrame(ts) OVER (PARTITION BY market_ticker ORDER BY ts, ingest_ts) AS prev_ts
      FROM {db}.orderbook_events
      WHERE ts >= now() - INTERVAL {int(hours)} HOUR
      GROUP BY market_ticker, ts, ingest_ts
    )
    SELECT
      market_ticker,
            max(dateDiff('second', prev_ts, ts)) AS max_gap_seconds,
            quantileExact(0.99)(dateDiff('second', prev_ts, ts)) AS p99_gap_seconds,
      count() AS points
    FROM ev
    WHERE prev_ts IS NOT NULL
    GROUP BY market_ticker
    ORDER BY max_gap_seconds DESC
    LIMIT 50
    """


def sql_seq_anomalies(db: str, hours: int) -> str:
    # best-effort: detect backwards seq and huge jumps (using ts ordering)
    return f"""
    WITH ev AS (
      SELECT
        market_ticker,
        ts,
        ingest_ts,
        seq,
        lagInFrame(seq) OVER (PARTITION BY market_ticker ORDER BY ts, ingest_ts) AS prev_seq
      FROM {db}.orderbook_events
      WHERE ts >= now() - INTERVAL {int(hours)} HOUR
      GROUP BY market_ticker, ts, ingest_ts, seq
    )
    SELECT
      market_ticker,
      countIf(prev_seq IS NOT NULL AND seq < prev_seq) AS backwards,
      countIf(prev_seq IS NOT NULL AND (seq - prev_seq) > 5000) AS huge_jumps,
      count() AS points
    FROM ev
    GROUP BY market_ticker
    HAVING backwards > 0 OR huge_jumps > 0
    ORDER BY backwards DESC, huge_jumps DESC
    LIMIT 50
    """


def sql_clock_skew(db: str, hours: int) -> str:
    return f"""
    SELECT
      market_ticker,
      quantileExact(0.01)(dateDiff('second', ts, ingest_ts)) AS p01_ingest_minus_ts_s,
      quantileExact(0.50)(dateDiff('second', ts, ingest_ts)) AS p50_ingest_minus_ts_s,
      quantileExact(0.99)(dateDiff('second', ts, ingest_ts)) AS p99_ingest_minus_ts_s,
      max(dateDiff('second', ts, ingest_ts)) AS max_ingest_minus_ts_s,
      min(dateDiff('second', ts, ingest_ts)) AS min_ingest_minus_ts_s
    FROM {db}.orderbook_events
    WHERE ts >= now() - INTERVAL {int(hours)} HOUR
    GROUP BY market_ticker
    ORDER BY max_ingest_minus_ts_s DESC
    LIMIT 50
    """


def sql_value_sanity(db: str, hours: int) -> str:
    # price is UInt16 in your schema, but keep “bounds check” anyway.
    # snapshots should never have negative qty; deltas should almost never be 0.
    return f"""
    SELECT
      market_ticker,
      type,
      count() AS n
    FROM {db}.orderbook_events
    WHERE ts >= now() - INTERVAL {int(hours)} HOUR
      AND (
        (type='snapshot' AND qty < 0)
        OR (type='delta' AND qty = 0)
        OR (price > 100)
      )
    GROUP BY market_ticker, type
    ORDER BY n DESC
    LIMIT 50
    """


# ----------------------------
# Report runner
# ----------------------------

def run_report(client: CHHTTPClient, db: str, hours: int, days: int) -> int:
    """
    Returns exit code: 0=healthy enough, 2=warnings found.
    (We keep it simple; you can tighten thresholds later.)
    """
    warn = 0

    def run_section(title: str, sql: str, max_rows: int = 30) -> List[Tuple[Any, ...]]:
        nonlocal warn
        print(f"\n=== {title} ===")
        _, rows = client.query(sql)
        print(_fmt_rows(rows, max_rows))
        return rows

    # 1) Is ingestion alive?
    run_section("QA: GLOBAL ROWS PER DAY", sql_global_rows_per_day(db, days), 30)
    last_seen = run_section("QA: PER-TICKER LAST SEEN (recent)", sql_last_seen_per_ticker(db, hours), 30)

    # 2) Identity integrity (this is the *most important*)
    nil_rows = run_section("QA: NIL UUID CHECK (must be 0)", sql_identity_nil_uuid(db, hours), 5)
    if nil_rows and nil_rows[0][0] and int(nil_rows[0][0]) > 0:
        warn += 1
        print("!! WARNING: nil UUID rows found. This pollutes analytics keys.")

    t2m = run_section("QA: TICKER -> MARKET_ID STABILITY (must be empty)", sql_ticker_to_market_id_stability(db, hours), 50)
    if t2m:
        warn += 1
        print("!! WARNING: a ticker mapped to multiple market_ids in-window (identity corruption risk).")

    m2t = run_section("QA: MARKET_ID -> TICKER STABILITY (must be empty)", sql_market_id_to_ticker_stability(db, hours), 50)
    if m2t:
        warn += 1
        print("!! WARNING: a market_id mapped to multiple tickers in-window (identity corruption risk).")

    # 3) Snapshot vs delta ratio (event-level)
    run_section("QA: SNAPSHOT vs DELTA EVENT RATIO (event-level)", sql_snapshot_delta_event_ratio(db, hours), 30)
    print("Interpretation: high snaps_per_delta_event can mean resnapshots or low delta activity.")

    # 4) Kalshi sid behavior (informational)
    sid_multi = run_section("QA: SID USED BY MULTIPLE TICKERS (informational)", sql_sid_multi_ticker(db, hours), 50)
    if sid_multi:
        print("Note: This is expected if Kalshi bundles subscriptions under one sid. It's OK if your streamer keys state by ticker.")

    # 5) Continuity + ordering
    run_section("QA: MAX GAP SECONDS PER TICKER", sql_max_gap_seconds_per_ticker(db, hours), 30)
    seq_anom = run_section("QA: SEQ ANOMALIES (best-effort)", sql_seq_anomalies(db, hours), 30)
    if seq_anom:
        warn += 1
        print("!! WARNING: seq backwards or huge jumps detected (could be reconnects, buffering, or ordering artifacts).")

    # 6) Clock skew
    run_section("QA: CLOCK SKEW (ingest_ts - ts)", sql_clock_skew(db, hours), 30)
    print("Interpretation: large positive = ingest lag/backpressure. Large negative = timestamp/parsing issues.")

    # 7) Value sanity
    vs = run_section("QA: VALUE SANITY (negative snapshot qty, delta=0, price>100)", sql_value_sanity(db, hours), 50)
    if vs:
        warn += 1
        print("!! WARNING: suspicious values present. Investigate streamer parsing or upstream anomalies.")

    print("\n=== QA COMPLETE ===")
    if warn:
        print(f"Result: WARN ({warn} sections raised warnings)")
        return 2
    print("Result: OK")
    return 0


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=os.getenv("CLICKHOUSE_DATABASE", "kalshi"))
    ap.add_argument("--hours", type=int, default=6, help="Lookback window for most checks")
    ap.add_argument("--days", type=int, default=14, help="Lookback window for global/day checks")
    ap.add_argument("--timeout", type=int, default=30, help="HTTP timeout seconds")
    args = ap.parse_args()

    hours = _hours(args.hours)
    days = _days(args.days)

    client = ch_client_from_env()
    # set timeout by monkey-patching if needed; kept simple
    rc = run_report(client, db=args.db, hours=hours, days=days)
    sys.exit(rc)


if __name__ == "__main__":
    main()
