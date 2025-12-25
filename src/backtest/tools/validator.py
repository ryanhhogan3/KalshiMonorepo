"""Quick validator to confirm qty sign convention and top-of-book orientation.

Usage:
    python -m backtest.tools.validator --market SAMPLE.MKT --minutes 2

Checks:
- count of positive qty vs negative qty in the window
- implied best bid-like (max price where qty>0) and best ask-like (min price where qty<0)
"""
from __future__ import annotations

import os
import argparse
import time
from datetime import datetime, timedelta
import requests
import json
from typing import Dict, Any


def _ch_url() -> str:
    return os.getenv("CH_URL", "http://localhost:8123")


def _query(sql: str):
    url = _ch_url()
    sql = sql.strip() + " FORMAT JSONEachRow"
    resp = requests.post(url, data=sql.encode("utf-8"), timeout=30)
    resp.raise_for_status()
    rows = []
    for line in resp.iter_lines(decode_unicode=True):
        if not line:
            continue
        try:
            rows.append(json.loads(line))
        except Exception:
            continue
    return rows


def run_check(market: str, minutes: int = 2):
    now = datetime.utcnow()
    start = now - timedelta(minutes=minutes)
    start_ms = int(start.timestamp() * 1000)
    end_ms = int(now.timestamp() * 1000)

    sql = (
        "SELECT price, qty FROM orderbook_events "
        f"WHERE market_ticker = '{market}' AND ts_ms >= {start_ms} AND ts_ms <= {end_ms} "
    )
    rows = _query(sql)

    pos = 0
    neg = 0
    prices_pos = []
    prices_neg = []

    for r in rows:
        qty = r.get('qty') or 0
        price = r.get('price')
        if qty > 0:
            pos += 1
            if price is not None:
                prices_pos.append(price)
        elif qty < 0:
            neg += 1
            if price is not None:
                prices_neg.append(price)

    print(f"Window: {start.isoformat()} -> {now.isoformat()} ({minutes}m)")
    print(f"rows: {len(rows)} | qty>0: {pos} | qty<0: {neg}")
    if prices_pos:
        print(f"implied best bid-like (max price where qty>0): {max(prices_pos)}")
    else:
        print("no positive qty rows in window")
    if prices_neg:
        print(f"implied best ask-like (min price where qty<0): {min(prices_neg)}")
    else:
        print("no negative qty rows in window")


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--market", required=True)
    parser.add_argument("--minutes", type=int, default=2)
    args = parser.parse_args(argv)
    run_check(args.market, args.minutes)


if __name__ == '__main__':
    main()
