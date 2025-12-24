"""Parquet reader for cached replay datasets."""
from __future__ import annotations

from typing import Generator, Dict, Any
import pandas as pd


def read_parquet_events(path: str) -> Generator[Dict[str, Any], None, None]:
    df = pd.read_parquet(path)
    # canonical ordering
    order_cols = ['ts_ms', 'seq', 'sid', 'side', 'price']
    existing = [c for c in order_cols if c in df.columns]
    if existing:
        df = df.sort_values(existing)
    else:
        df = df.sort_values('ts_ms')

    for row in df.to_dict(orient='records'):
        yield row
