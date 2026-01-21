# KalshiMonorepo
The monorepo for Hogan's connection to the Kalshi API and websocket system

# KalshiMonorepo — Realtime Order Book Ingest, Analytics, and Archive

This project streams **Kalshi** order book data over WebSocket, persists it to a **hot analytics store (ClickHouse)** for sub-second queries, and simultaneously archives a **cold, lossless history (Parquet)** for backtests and ML.

- **Hot (ClickHouse):** live dashboards, spreads, depth, imbalance, rolling stats
- **Cold (Parquet):** cheap, portable, replayable event history

> ⚠️ You must have a Kalshi production API Key ID and a matching RSA **private key (PEM)**.

---

## Quick Start (Windows PowerShell)

> Start fresh (even if you removed the Docker container). Run from the repo root.

```powershell
# venv + deps
python -m venv venv
.\venv\Scripts\Activate.ps1
pip install -U pip
pip install -r requirements.txt   # or: pip install -e .

# Required environment (same shell session!)
$env:PROD_KEYID       = "<YOUR_KALSHI_KEY_ID>"
$env:PROD_KEYFILE     = "C:\full\path\to\your\kalshi_private_key.pem"
$env:MARKET_TICKERS   = "KXBTCMAXY-25-DEC31-129999.99"

$env:CLICKHOUSE_URL       = "http://localhost:8123"
$env:CLICKHOUSE_USER      = "default"
$env:CLICKHOUSE_PASSWORD  = "secret123"
$env:CLICKHOUSE_DATABASE  = "kalshi"
$env:USE_CLICKHOUSE       = "1"
$env:PARQUET_DIR          = ".\data"

# Fresh ClickHouse
docker rm -f ch 2>$null
docker run -d --name ch -p 8123:8123 -p 9000:9000 `
  -e CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1 `
  -e CLICKHOUSE_USER=default `
  -e CLICKHOUSE_PASSWORD=secret123 `
  clickhouse/clickhouse-server:24.8

# Create DB + tables (PowerShell-friendly)
Get-Content .\src\databases\storage\clickhouse_ddl.sql -Raw |
  docker exec -i ch clickhouse-client --multiquery -u default --password secret123

# Sanity
curl.exe -u default:secret123 "http://localhost:8123/?query=SELECT%201"
curl.exe -u default:secret123 "http://localhost:8123/?query=SHOW%20TABLES%20FROM%20kalshi"

# Live wire only (no DB writes): expect [SUB], [SNAP], then deltas
python -m kalshi.websocket.market_worker --ticker KXBTCMAXY-25-DEC31-129999.99 --compact --rate 5

# Full pipeline (WS → ClickHouse + Parquet)
python -m kalshi.websocket.streamer

# Verify data landed
curl.exe -u default:secret123 "http://localhost:8123/?query=SELECT%20count()%20FROM%20kalshi.orderbook_events"

## Market-Maker Inventory Semantics

### Definitions

- `pos_filled` is **only** the reconciled fill-based position from the exchange; it never includes working orders.
- `pos_open_exposure` is recomputed each engine loop from currently live working orders.
- `pos_total_est = pos_filled + pos_open_exposure` and drives hysteresis + block logic.
- `open_orders_count`, `open_yes_bid_size`, `open_yes_ask_size`, `open_no_bid_size`, and `open_no_ask_size` capture the exact resting inventory on every `mm_decisions` row.
- `allowed`, `block_stage`, and `block_codes` describe whether quoting was permitted. `block_codes` is the authoritative list of reasons; `reason_codes` remains for legacy/partial tags and may repeat or omit information.
- `inv_convention` records the normalization used when reconciling fills so exposure math can be reproduced offline.

> ⚠️ **Do not roll inventory.** Never set `inv_before := inv_after_est` or similar shortcuts. Inspect `pos_filled`, recompute `pos_open_exposure`, then derive `pos_total_est` every loop.

### Where to dig

- `kalshi.mm_decisions` now hosts the `pos_*` fields above along with `allowed`, `block_stage`, `block_codes`, and `reason_codes`.
- `kalshi.mm_order_actions` links every placement/cancel to its `request_json`, so you can see whether the engine attempted to adjust the book in that second.
- `kalshi.mm_order_responses` surfaces `status`, `reject_reason`, and `response_json` for ACK/NACK context.
- `kalshi.mm_position_snapshots` (see [infra/clickhouse_migrations/010_inventory_observability.sql](infra/clickhouse_migrations/010_inventory_observability.sql)) is the ground-truth filled position; compare it directly against `pos_filled`.

## Inventory QA Runbook

1. **Apply the migration (idempotent).** From any host with the `chq` helper:

   ```bash
   chq "SOURCE /app/infra/clickhouse_migrations/010_inventory_observability.sql"
   ```

2. **Run the global QA sweep (3-hour lookback):**

   ```bash
   python -m qa.mm_inventory_qa \
     --host localhost --port 9000 --user default --password default_password --database kalshi \
     --lookback-minutes 180
   ```

3. **Validate a specific ticker (adjust lookback + filter as needed):**

   ```bash
   python -m qa.mm_inventory_qa \
     --host localhost --port 9000 --user default --password default_password --database kalshi \
     --market-ticker KXBTCD-26JAN2317-T93999.99 --lookback-minutes 240
   ```

4. **Interpret the output (exit code `2` when anomalies exist):**

   - *Check A* (`Drift Without Actions`): buckets where `pos_total_est` moved ≥ `--drift-threshold` contracts while `mm_order_actions` recorded zero placements/cancels.
   - *Check B* (`Open Exposure Mismatch`): rows where `open_orders_count = 0` yet `|pos_open_exposure| > --open-epsilon` (default 0.01).
   - *Check C* (`Decision vs Snapshot Divergence`): joins `mm_decisions.pos_filled` to the latest `mm_position_snapshots.pos_filled` per ticker and flags deltas above `--filled-threshold`. The script prints a skip notice (exit 0) if the snapshot table is absent.

5. **Investigate offenders.** Filter `kalshi.mm_decisions` on the reported `bucket_second` or `ts`, review `allowed/block_codes` to understand the gate, and correlate with `mm_order_actions.request_json` plus `mm_order_responses.reject_reason` for transport errors.

The helper accepts env vars (`CH_HOST`, `CH_PORT`, etc.) for defaults, prints deterministic tables, exits `1` on ClickHouse connectivity/query failures, and caps each check at `--limit` rows (default 200).
