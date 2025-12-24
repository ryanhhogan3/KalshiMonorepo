"""Minimal backtest package for KalshiMonorepo.

This package provides a ClickHouse reader (HTTP), Parquet reader/cache,
an event stream wrapper, a tiny book builder, and a CLI entrypoint for
running quick deterministic backtests locally.

Note: ClickHouse query SQL is intentionally generic; adapt column names to
match your ClickHouse schema if needed. The included fixture generator
creates a small synthetic Parquet dataset for local smoke tests.
"""

__all__ = [
    "cli",
    "data",
    "replay",
]
