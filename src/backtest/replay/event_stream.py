"""Event stream wrapper that selects ClickHouse or Parquet source and yields events."""
from __future__ import annotations

import os
from datetime import datetime
from typing import Generator, Dict, Any, Optional, List

from backtest.data import clickhouse_reader
from backtest.data import parquet_reader


def _group_snapshot_rows(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Group snapshot rows (same seq) into a snapshot batch dict."""
    if not rows:
        return {}
    seq = rows[0].get('seq')
    ts = rows[0].get('ts_ms')
    return {"kind": "snapshot", "seq": seq, "ts_ms": ts, "rows": rows}


def _to_unix_ms(t: datetime) -> int:
    return int(t.timestamp() * 1000)


def get_event_stream(source: str, market: str, t_start: datetime, t_end: datetime, fixture_path: Optional[str] = None, cache: bool = False) -> Generator[Dict[str, Any], None, None]:
    """Return iterator yielding snapshot batches and delta events.

    Behavior:
    - If source == 'clickhouse', query ClickHouse to find the last snapshot seq before t_start,
      yield the snapshot batch, then stream the following deltas until t_end.
    - If source == 'parquet', read the parquet file and yield snapshot batches/deltas by grouping rows with same seq and type.
    - If cache=True and source=='clickhouse', the caller may choose to write cache after fetching (handled by caller/CLI).
    """
    start_ms = _to_unix_ms(t_start)
    end_ms = _to_unix_ms(t_end)

    if source == 'clickhouse':
        snap_meta = clickhouse_reader.get_snapshot_before(market, start_ms)
        if not snap_meta:
            raise RuntimeError("No snapshot found at or before t_start")

        snapshot_seq = snap_meta['seq']
        snapshot_ts = snap_meta['ts_ms']

        # fetch rows belonging to snapshot
        snap_rows = clickhouse_reader.fetch_snapshot_rows(market, snapshot_seq)
        yield _group_snapshot_rows(snap_rows)

        # stream all events from snapshot_ts (inclusive) to end_ms
        rows = list(clickhouse_reader.fetch_window(market, snapshot_ts, end_ms))
        # optionally write cache if requested via CLI flag; the CLI will call write_cache based on this stream
        for row in rows:
            # skip rows that are part of the snapshot seq (they were already yielded)
            if row.get('seq') == snapshot_seq and row.get('type') == 'snapshot':
                continue
            yield {"kind": "delta", "ts_ms": row.get('ts_ms'), "seq": row.get('seq'), "row": row}

    elif source == 'parquet':
        if not fixture_path:
            raise ValueError("fixture_path required for parquet source")
        # read all rows and group snapshot rows by seq
        rows = list(parquet_reader.read_parquet_events(fixture_path))
        # filter to window
        rows = [r for r in rows if r.get('ts_ms', 0) >= start_ms and r.get('ts_ms', 0) <= end_ms]

        # group snapshot rows by seq
        snapshot_groups: Dict[int, List[Dict[str, Any]]] = {}
        for r in rows:
            if r.get('type') == 'snapshot':
                seq = r.get('seq')
                snapshot_groups.setdefault(seq, []).append(r)

        # yield the earliest snapshot group (the one with the lowest ts <= start)
        if snapshot_groups:
            earliest_seq = min(snapshot_groups.keys())
            yield _group_snapshot_rows(sorted(snapshot_groups[earliest_seq], key=lambda x: (x.get('ts_ms'), x.get('seq'), x.get('sid'))))

        for r in rows:
            if r.get('type') == 'snapshot':
                # already handled
                continue
            yield {"kind": "delta", "ts_ms": r.get('ts_ms'), "seq": r.get('seq'), "row": r}
    else:
        raise ValueError(f"unknown source: {source}")
