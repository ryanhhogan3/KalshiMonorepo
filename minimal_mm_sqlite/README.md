# minimal_mm_sqlite

A tiny, plug-and-play Kalshi market maker scaffold that:

- Quotes **both legs** (BUY YES bid + BUY NO bid) when allowed
- Cancels its own open orders when blocked
- Persists decisions/actions/responses/positions into a local **SQLite** DB
- Uses the existing repo WebSocket runtime for market data (no ClickHouse)

## Quick start

1) Create a `.env` file (in this directory) based on `.env.example`.

2) Put one ticker per line in `markets.txt`.

3) Run:

```bash
python -m minimal_mm_sqlite.run
```

## Notes

- This is intentionally minimal: it’s a starter you can extend.
- It uses the repo’s WebSocket client which expects `PROD_KEYID` and `PROD_KEYFILE` for signing.
- Trading can be disabled via `MM_TRADING_ENABLED=0` (paper mode). In paper mode, the engine logs actions/responses but does not hit REST order endpoints.
