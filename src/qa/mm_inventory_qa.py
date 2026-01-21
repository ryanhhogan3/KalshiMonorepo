"""Deterministic ClickHouse QA checks for market-maker inventory drift."""
from __future__ import annotations

import argparse
import os
import sys
from typing import List, Sequence, Tuple

try:
    import clickhouse_connect
except ImportError as exc:  # pragma: no cover
    raise SystemExit("clickhouse_connect is required to run mm_inventory_qa.py") from exc


SQL_DRIFT_WITHOUT_ACTIONS = """
WITH decisions AS (
    SELECT
        toStartOfSecond(ts) AS bucket_second,
        market_ticker,
        min(pos_total_est) AS inv_min,
        max(pos_total_est) AS inv_max,
        count() AS n_decisions
    FROM kalshi.mm_decisions
    WHERE ts >= now() - INTERVAL %(lookback)s MINUTE
      {decisions_filter}
    GROUP BY bucket_second, market_ticker
),
actions AS (
    SELECT
        toStartOfSecond(ts) AS bucket_second,
        market_ticker,
        count() AS n_actions
    FROM kalshi.mm_order_actions
    WHERE ts >= now() - INTERVAL %(lookback)s MINUTE
      {actions_filter}
    GROUP BY bucket_second, market_ticker
)
SELECT
    d.bucket_second,
    d.market_ticker,
    d.inv_min,
    d.inv_max,
    d.inv_max - d.inv_min AS delta,
    d.n_decisions,
    ifNull(a.n_actions, 0) AS n_actions
FROM decisions d
LEFT JOIN actions a
    ON a.bucket_second = d.bucket_second
   AND a.market_ticker = d.market_ticker
WHERE ifNull(a.n_actions, 0) = 0
  AND abs(d.inv_max - d.inv_min) >= %(threshold)s
ORDER BY d.bucket_second DESC
LIMIT %(limit)s
"""


SQL_OPEN_EXPOSURE_MISMATCH = """
SELECT
    ts,
    market_ticker,
    pos_filled,
    pos_open_exposure,
    pos_total_est,
    open_orders_count
FROM kalshi.mm_decisions
WHERE ts >= now() - INTERVAL %(lookback)s MINUTE
  {decisions_filter}
  AND open_orders_count = 0
  AND abs(pos_open_exposure) > %(epsilon)s
ORDER BY ts DESC
LIMIT %(limit)s
"""


SQL_SNAPSHOT_DIVERGENCE = """
WITH latest_snapshots AS (
    SELECT
        market_ticker,
        argMax(pos_filled, ts) AS pos_filled
    FROM kalshi.mm_position_snapshots
    WHERE ts >= now() - INTERVAL %(lookback)s MINUTE
      {snapshot_filter}
    GROUP BY market_ticker
)
SELECT
    d.ts,
    d.market_ticker,
    d.pos_filled AS decision_pos_filled,
    s.pos_filled AS snapshot_pos_filled,
    abs(d.pos_filled - s.pos_filled) AS delta
FROM kalshi.mm_decisions d
INNER JOIN latest_snapshots s ON s.market_ticker = d.market_ticker
WHERE d.ts >= now() - INTERVAL %(lookback)s MINUTE
  {decisions_filter}
  AND abs(d.pos_filled - s.pos_filled) > %(threshold)s
ORDER BY delta DESC, d.ts DESC
LIMIT %(limit)s
"""


SQL_SNAPSHOT_TABLE_EXISTS = """
SELECT count()
FROM system.tables
WHERE database = %(database)s
  AND name = 'mm_position_snapshots'
"""


def _env_default(name: str, fallback: str) -> str:
    return os.environ.get(name.upper(), fallback)


def _env_flag(name: str) -> bool:
    return os.environ.get(name.upper(), "").lower() in {"1", "true", "yes"}


def build_client(args: argparse.Namespace):
    protocol = args.protocol or os.environ.get("CH_PROTOCOL")
    if not protocol:
        protocol = "native" if args.port == 9000 else "http"
    return clickhouse_connect.get_client(
        host=args.host,
        port=args.port,
        username=args.user,
        password=args.password,
        database=args.database,
        interface=protocol,
        secure=args.secure,
    )


def run_query(client, sql: str, params: dict) -> Tuple[Sequence[str], List[Tuple]]:
    result = client.query(sql, parameters=params)
    return result.column_names, result.result_rows or []


def drift_without_actions(client, args):
    sql = SQL_DRIFT_WITHOUT_ACTIONS.format(
        decisions_filter="AND market_ticker = %(market_ticker)s" if args.market_ticker else "",
        actions_filter="AND market_ticker = %(market_ticker)s" if args.market_ticker else "",
    )
    params = {
        "lookback": args.lookback_minutes,
        "threshold": args.drift_threshold,
        "limit": args.limit,
    }
    if args.market_ticker:
        params["market_ticker"] = args.market_ticker
    return run_query(client, sql, params)


def open_exposure_mismatch(client, args):
    sql = SQL_OPEN_EXPOSURE_MISMATCH.format(
        decisions_filter="AND market_ticker = %(market_ticker)s" if args.market_ticker else "",
    )
    params = {
        "lookback": args.lookback_minutes,
        "epsilon": args.open_epsilon,
        "limit": args.limit,
    }
    if args.market_ticker:
        params["market_ticker"] = args.market_ticker
    return run_query(client, sql, params)


def snapshot_table_exists(client, database: str) -> bool:
    _, rows = run_query(client, SQL_SNAPSHOT_TABLE_EXISTS, {"database": database})
    return bool(rows and rows[0][0])


def snapshot_divergence(client, args):
    sql = SQL_SNAPSHOT_DIVERGENCE.format(
        snapshot_filter="AND market_ticker = %(market_ticker)s" if args.market_ticker else "",
        decisions_filter="AND market_ticker = %(market_ticker)s" if args.market_ticker else "",
    )
    params = {
        "lookback": args.lookback_minutes,
        "threshold": args.filled_threshold,
        "limit": args.limit,
    }
    if args.market_ticker:
        params["market_ticker"] = args.market_ticker
    return run_query(client, sql, params)


def _fmt(value) -> str:
    if isinstance(value, float):
        return f"{value:.6f}".rstrip("0").rstrip(".") or "0"
    return str(value)


def _print_table(title: str, columns: Sequence[str], rows: List[Tuple], limit: int) -> bool:
    print(f"\n{title}")
    if not rows:
        print("OK (no anomalies within lookback window)")
        return False
    header = " | ".join(columns)
    print(header)
    print("-" * len(header))
    for row in rows:
        print(" | ".join(_fmt(value) for value in row))
    if len(rows) >= limit:
        print(f"(truncated to {limit} rows)")
    return True


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="ClickHouse QA checks for MM inventory semantics")
    parser.add_argument('--host', default=_env_default('CH_HOST', 'localhost'), help='ClickHouse host (default: env CH_HOST or localhost)')
    parser.add_argument('--port', type=int, default=int(_env_default('CH_PORT', '9000')), help='ClickHouse port (default: env CH_PORT or 9000)')
    parser.add_argument('--user', default=_env_default('CH_USER', 'default'), help='ClickHouse user (default: env CH_USER or default)')
    parser.add_argument('--password', default=_env_default('CH_PASSWORD', ''), help='ClickHouse password (default: env CH_PASSWORD)')
    parser.add_argument('--database', default=_env_default('CH_DATABASE', 'kalshi'), help='ClickHouse database (default: env CH_DATABASE or kalshi)')
    parser.add_argument('--protocol', choices=('http', 'native'), default=os.environ.get('CH_PROTOCOL'), help='Protocol/interface to use (auto if omitted)')
    parser.add_argument('--secure', action='store_true', default=_env_flag('CH_SECURE'), help='Use HTTPS for HTTP protocol (default: env CH_SECURE)')
    parser.add_argument('--market-ticker', help='Optional single market_ticker filter')
    parser.add_argument('--lookback-minutes', type=int, default=120, help='Look-back window in minutes (default: 120)')
    parser.add_argument('--drift-threshold', type=float, default=1.0, help='Min contracts to flag drift without actions (default: 1)')
    parser.add_argument('--open-epsilon', type=float, default=0.01, help='Allowed open exposure when open_orders_count=0 (default: 0.01)')
    parser.add_argument('--filled-threshold', type=float, default=0.5, help='Allowed delta between decisions and snapshots (default: 0.5)')
    parser.add_argument('--limit', type=int, default=200, help='Max rows per check (default: 200)')
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        client = build_client(args)
    except Exception as exc:  # pragma: no cover
        print(f"ERROR: unable to connect to ClickHouse ({exc})", file=sys.stderr)
        return 1

    anomalies = False
    try:
        cols, rows = drift_without_actions(client, args)
        anomalies |= _print_table('Check A - Drift Without Actions', cols, rows, args.limit)

        cols, rows = open_exposure_mismatch(client, args)
        anomalies |= _print_table('Check B - Open Exposure Mismatch', cols, rows, args.limit)

        if snapshot_table_exists(client, args.database):
            cols, rows = snapshot_divergence(client, args)
            anomalies |= _print_table('Check C - Decision vs Snapshot Divergence', cols, rows, args.limit)
        else:
            print('\nCheck C - Decision vs Snapshot Divergence')
            print('SKIPPED (kalshi.mm_position_snapshots not found; run migration first)')
    except Exception as exc:  # pragma: no cover
        print(f"ERROR: query execution failed ({exc})", file=sys.stderr)
        return 1
    finally:
        try:
            client.close()
        except Exception:  # pragma: no cover
            pass

    if anomalies:
        print('\nAnomalies detected. See sections above.')
        return 2

    print('\nAll MM inventory QA checks passed.')
    return 0


if __name__ == '__main__':
    sys.exit(main())
