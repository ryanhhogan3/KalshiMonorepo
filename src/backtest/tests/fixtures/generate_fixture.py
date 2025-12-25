"""Generate a small deterministic parquet fixture for smoke tests.

This script requires `pandas` and `pyarrow` to be installed.
It writes a tiny dataset (≈1 minute) with a snapshot and a few updates/trades.
"""
from __future__ import annotations

import pandas as pd
import numpy as np
from datetime import datetime, timedelta


def write_sample_parquet(path: str) -> None:
    # Build a tiny deterministic sequence that matches `orderbook_events` schema
    # Schema columns: type, sid, seq, market_ticker, side, price, qty, ts_ms, ingest_ts
    start = datetime(2025, 11, 1, 10, 0, 0)
    rows = []

    # create a snapshot seq (seq = 100)
    seq = 100
    sid = 1
    t0 = start
    ingest_ts = int((t0 + timedelta(seconds=0)).timestamp() * 1000)

    # snapshot rows: several price levels on both sides
    snapshot_levels = [
        {'side': 'yes', 'price': 99.5, 'qty': 50},
        {'side': 'yes', 'price': 99.0, 'qty': 100},
        {'side': 'no', 'price': 100.5, 'qty': -60},
        {'side': 'no', 'price': 101.0, 'qty': -120},
    ]

    for i, lvl in enumerate(snapshot_levels):
        rows.append({
            'market_ticker': 'SAMPLE.MKT',
            'type': 'snapshot',
            'sid': sid,
            'seq': seq,
            'ts_ms': int(t0.timestamp() * 1000),
            'ingest_ts': ingest_ts,
            'side': lvl['side'],
            'price': lvl['price'],
            'qty': lvl['qty'],
        })

    # generate deltas after the snapshot (seq increases)
    for i in range(1, 13):
        t = t0 + timedelta(seconds=5 * i)
        ts_ms = int(t.timestamp() * 1000)
        seq_i = seq + i
        if i % 2 == 0:
            # update bid level
            rows.append({
                'market_ticker': 'SAMPLE.MKT',
                'type': 'delta',
                'sid': sid,
                'seq': seq_i,
                'ts_ms': ts_ms,
                'ingest_ts': ts_ms,
                'side': 'yes',
                'price': 99.0 + 0.01 * i,
                'qty': 100 - i,
            })
        else:
            # update ask level
            rows.append({
                'market_ticker': 'SAMPLE.MKT',
                'type': 'delta',
                'sid': sid,
                'seq': seq_i,
                'ts_ms': ts_ms,
                'ingest_ts': ts_ms,
                'side': 'no',
                'price': 101.0 - 0.01 * i,
                'qty': -(100 - i),
            })

    df = pd.DataFrame(rows)
    df.to_parquet(path, index=False)


if __name__ == '__main__':
    import sys, os
    out = sys.argv[1] if len(sys.argv) > 1 else os.path.join('tests', 'fixtures', 'sample_market_1min.parquet')
    os.makedirs(os.path.dirname(out), exist_ok=True)
    write_sample_parquet(out)
    print('Wrote fixture to', out)
