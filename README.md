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
