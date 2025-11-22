# KalshiMonorepo Project Architecture & Deployment Guide

## Project Overview

This is a **real-time Kalshi options market data ingestion and storage system** designed to:
1. Connect to Kalshi's WebSocket API to receive live orderbook updates
2. Build and maintain local orderbook state
3. Persist data to both ClickHouse (hot storage) and Parquet files (cold storage)
4. Support backtesting and market analysis

---

## Folder Structure & File Interactions

### `kalshifolder/websocket/` - Real-time Data Collection

#### **`ws_runtime.py`** (Core WebSocket Client)
- **Purpose**: Manages persistent WebSocket connection to Kalshi API
- **Key Components**:
  - `KalshiWSRuntime` class: owns a single WS connection
  - RSA signature generation for authentication (uses `PROD_KEYID` + `PROD_KEYFILE`)
  - Automatic reconnection with exponential backoff
  - Async queue for frame distribution (max 50,000 frames)
  - Heartbeat/ping management (20-second intervals)

- **Dependencies**: 
  - `websockets` (async WebSocket client)
  - `cryptography` (RSA signing)
  - Environment: `PROD_KEYID`, `PROD_KEYFILE`, `WS_URL`, `WS_HOSTNAME`

- **Key Methods**:
  - `async start()`: Idempotently start the WS loop
  - `async subscribe_markets(tickers)`: Send subscription payload for orderbook deltas
  - `async send(payload)`: Send raw JSON to Kalshi
  - `async wait_connected(timeout)`: Wait for connection readiness
  - `property is_connected`: Check connection status

- **Output**: Raw JSON strings pushed to `self.queue`

---

#### **`order_book.py`** (State Management)
- **Purpose**: Maintains local order book state for each market
- **Key Classes**:
  - `SideBook`: Tracks price levels and sizes for "yes" or "no" side
    - `levels`: dict mapping price (cents) → size
    - `apply_delta()`: Updates size at a price level
  - `OrderBook`: Full orderbook state for a single market
    - `market_id`, `ticker`: Market identifiers
    - `yes`, `no`: Two `SideBook` instances
    - `last_seq`: Sequence number for delta validation

- **Key Methods**:
  - `apply_snapshot()`: Replaces entire book from a snapshot message
  - `apply_delta()`: Applies a single price level change
  - `size_at()`: Query current size at a price

- **No Dependencies** (pure Python)

---

#### **`streamer.py`** (Main Orchestrator)
- **Purpose**: Glues together WebSocket, orderbook, and persistence layers
- **Flow**:
  1. Load environment variables (`.env`)
  2. Instantiate `KalshiWSRuntime` → connects to Kalshi
  3. Instantiate `ClickHouseSink` and `ParquetSink` for persistence
  4. Wait for WebSocket connection
  5. Subscribe to markets (from `MARKET_TICKERS`)
  6. Main loop:
     - Pop raw JSON from `rt.queue`
     - Parse frames (subscribed, orderbook_snapshot, orderbook_delta)
     - Update `OrderBook` objects
     - Push snapshot/delta rows to both sinks

- **Frame Types Handled**:
  - `subscribed`: Acknowledgment of subscription (contains `sid`)
  - `orderbook_snapshot`: Full orderbook state (initial load)
  - `orderbook_delta`: Single level change (price, delta, side)

- **Environment Variables**:
  - `MARKET_TICKERS`: Comma-separated list of markets
  - `PARQUET_DIR`, `CLICKHOUSE_URL`, `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD`, `CLICKHOUSE_DATABASE`
  - `USE_ORJSON`: Enable fast JSON parsing (optional)

- **Output**: Rows sent to both ClickHouse (batch insert) and Parquet (file writes)

---

#### **`market_worker.py`** (Single-Market Viewer)
- **Purpose**: Standalone tool to monitor a single market
- **Features**:
  - Pretty-print or compact JSON
  - Rate limiting for output
  - NDJSON file logging
  - Snapshot/delta processing

- **Usage**:
  ```bash
  python -m kalshifolder.websocket.market_worker --ticker FEDFUNDS-DEC25 --compact
  ```

---

### `databases/processing/` - Persistence Layer

#### **`clickhouse_sink.py`** (Hot Storage - Real-time Writes)
- **Purpose**: High-performance batch insertion into ClickHouse
- **Class**: `ClickHouseSink`
- **Tables**:
  - `kalshi.orderbook_events`: All snapshots and deltas (historical truth)
    - Columns: `type`, `sid`, `seq`, `market_id`, `market_ticker`, `side`, `price`, `qty`, `ts`, `ingest_ts`
  - `kalshi.latest_levels`: Current best bid/ask for each market (ReplacingMergeTree)
    - Columns: `market_id`, `market_ticker`, `side`, `price`, `size`, `ts`, `ingest_ts`

- **Behavior**:
  - Buffers rows in memory by column
  - Flushes when `BATCH_ROWS` (default 2000) rows accumulated OR `BATCH_FLUSH_SECS` (default 0.010) elapsed
  - Handles multiple `clickhouse-connect` API versions (columnar vs row-oriented)

- **Environment**:
  - `CLICKHOUSE_URL`, `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD`, `CLICKHOUSE_DATABASE`
  - `BATCH_ROWS`, `BATCH_FLUSH_SECS`

- **Key Methods**:
  - `async add_event(row)`: Add a snapshot/delta row
  - `async upsert_latest(row)`: Update latest price level
  - `async flush()`: Force insert all buffered rows

---

#### **`parquet_sink.py`** (Cold Storage - Archival)
- **Purpose**: Archive orderbook data to Parquet files (optimized for queries)
- **Class**: `ParquetSink`
- **Behavior**:
  - Buffers rows in memory as list of dicts
  - Flushes to a new Parquet file ONLY when buffer size exceeds `max_bytes` (default ~100MB)
  - Partitioned by date in directory structure: `./data/orderbook/date=YYYY-MM-DD/`
  - Files named: `part-HHmmss_microseconds.parquet`

- **Compression**: ZSTD (good compression ratio)

- **Size Estimation**:
  - Uses PyArrow in-memory table to estimate uncompressed size
  - Multiplies by `compression_ratio` (default 0.35) to estimate on-disk size
  - Checks size every `size_check_every` rows (default 1000) to avoid overhead

- **Environment**:
  - `PARQUET_DIR`, `PARQUET_MAX_BYTES` (or hardcoded 100MB)

- **Key Methods**:
  - `async add(row)`: Append row, check if flush needed
  - `async flush()`: Write buffer to Parquet file

---

### `databases/storage/` - Schema Definition

#### **`clickhouse_ddl.sql`** (Database Schema)
- **Tables**:
  1. **`kalshi.orderbook_events`**
     - MergeTree engine (most performant for write-heavy workloads)
     - Partitioned by month (`toYYYYMM(ts)`)
     - Order key: `(market_id, side, price, ts, seq)` (optimizes range queries)
     - Codecs: `LZ4`, `Delta`, `ZSTD` (space-efficient storage)
     - Index granularity: 8192

  2. **`kalshi.latest_levels`**
     - ReplacingMergeTree engine (auto-deduplicates by `ts`)
     - Order key: `(market_id, side, price)`
     - Used for "current best bid/ask" queries

- **Data Types**:
  - `Enum8` for `type` ('snapshot'=1, 'delta'=2) and `side` ('yes'=1, 'no'=2)
  - `UUID` for `market_id`
  - `LowCardinality(String)` for `market_ticker` (memory-efficient)
  - `UInt16` for `price` (0-10000 cents = $0-$100)
  - `Int32` for `qty` (signed, supports negatives for deltas)
  - `DateTime64(6, 'UTC')` for timestamps (microsecond precision)

---

### `kalshifolder/api/` - REST Integration (Optional)

#### **`test_REST_connection.py`**
- Standalone test for REST API connectivity (not used by main streamer)

---

## Data Flow Diagram

```
┌─────────────────────┐
│ Kalshi WebSocket    │
│ API                 │
└──────────┬──────────┘
           │ (orderbook frames)
           │
    ┌──────▼──────┐
    │ ws_runtime  │
    │ .queue      │
    └──────┬──────┘
           │
    ┌──────▼──────────┐
    │ streamer.py     │
    │ (main loop)     │
    └─┬────────┬──────┘
      │        │
  ┌───▼──┐  ┌──▼────────┐
  │order_│  │ clickhouse │
  │book  │  │_sink      │
  └──┬───┘  └──┬────────┘
     │         │ (batch)
     │     ┌───▼──────────┐
     │     │ClickHouse    │
     │     │ orderbook_    │
     │     │ events table  │
     │     └──────────────┘
     │
  ┌──▼─────────────┐
  │ parquet_sink   │
  │ (cold storage) │
  └──┬─────────────┘
     │ (only when file size > 100MB)
     │
  ┌──▼──────────────────┐
  │ ./data/orderbook/   │
  │ date=YYYY-MM-DD/    │
  │ part-*.parquet      │
  └─────────────────────┘
```

---

## Environment Configuration (`.env`)

```bash
# Authentication
PROD_KEYID=<your-kalshi-api-key-id>
PROD_KEYFILE=<path-to-private-key-pem>

# Markets
MARKET_TICKERS=KXBTCMAXY-25-DEC31-129999.99,FEDFUNDS-DEC25

# WebSocket
WS_URL=wss://api.elections.kalshi.com/trade-api/ws/v2
WS_HOSTNAME=api.elections.kalshi.com

# ClickHouse (hot storage)
CLICKHOUSE_URL=http://localhost:8123
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=<password>
CLICKHOUSE_DATABASE=kalshi

# Batching
BATCH_ROWS=2000
BATCH_FLUSH_SECS=0.010

# Parquet (cold storage)
PARQUET_DIR=./data
PARQUET_MAX_BYTES=104857600  # ~100MB

# Optional
USE_ORJSON=1
```

---

## Cloud Deployment Checklist

### Prerequisites
1. **ClickHouse Instance**:
   - Install ClickHouse server (Linux recommended)
   - Run `clickhouse_ddl.sql` to create database and tables
   - Enable network access from application server
   - Configure user credentials

2. **Python Environment**:
   - Python 3.9+
   - Dependencies: `websockets`, `cryptography`, `certifi`, `clickhouse-connect`, `pyarrow`, `pandas`, `python-dotenv`, `orjson` (optional)

3. **Kalshi API Credentials**:
   - Obtain API key ID and RSA private key from Kalshi
   - Store private key securely (e.g., in AWS Secrets Manager or environment)

### Deployment Steps

1. **Provision Cloud Instance** (AWS EC2, GCP VM, or equivalent):
   - OS: Ubuntu 20.04 LTS or later
   - CPU: 2+ cores recommended
   - Memory: 4GB minimum (8GB for high-throughput)
   - Storage: 50GB+ for Parquet files

2. **Install ClickHouse**:
   ```bash
   curl https://clickhouse.com/ | sh
   sudo ./clickhouse install
   sudo systemctl start clickhouse-server
   ```

3. **Create Database Schema**:
   ```bash
   clickhouse-client < src/databases/storage/clickhouse_ddl.sql
   ```

4. **Deploy Application**:
   ```bash
   git clone https://github.com/ryanhhogan3/KalshiMonorepo.git
   cd KalshiMonorepo
   python -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   ```

5. **Configure Environment**:
   - Create `.env` in project root
   - Set `PROD_KEYID`, `PROD_KEYFILE`, `CLICKHOUSE_URL`, etc.
   - For cloud: use service discovery (e.g., `CLICKHOUSE_URL=http://clickhouse-service:8123`)

6. **Start Data Ingestion**:
   ```bash
   python -m kalshifolder.websocket.streamer
   ```

7. **Monitor**:
   - Query ClickHouse to verify data is flowing:
     ```sql
     SELECT market_ticker, COUNT(*) as rows, MAX(ts) as last_update 
     FROM kalshi.orderbook_events 
     GROUP BY market_ticker;
     ```
   - Check Parquet file creation in `./data/orderbook/`
   - Monitor ClickHouse memory/disk usage

### Scaling Considerations
- **Multiple Markets**: Increase `BATCH_ROWS` if ingestion rate is high
- **Network Latency**: Adjust `WS_HOSTNAME` to geographically closer server
- **Storage**: Parquet files grow ~100MB per 5 minutes at high frequency; plan disk space accordingly
- **ClickHouse Replication**: For high availability, configure ClickHouse replication clusters

---

## Querying Data

### ClickHouse Examples
```sql
-- Latest bid/ask for a market
SELECT side, price, size FROM kalshi.latest_levels 
WHERE market_ticker = 'FEDFUNDS-DEC25' 
ORDER BY price DESC;

-- Historical orderbook snapshots
SELECT ts, side, price, qty FROM kalshi.orderbook_events 
WHERE market_ticker = 'FEDFUNDS-DEC25' 
  AND type = 'snapshot' 
ORDER BY ts DESC 
LIMIT 100;

-- Orderbook evolution (deltas)
SELECT ts, seq, side, price, qty FROM kalshi.orderbook_events 
WHERE market_ticker = 'FEDFUNDS-DEC25' 
  AND type = 'delta' 
ORDER BY seq;
```

### Parquet (Local Analysis)
```python
import pyarrow.parquet as pq
table = pq.read_table('./data/orderbook/date=2025-11-05/part-*.parquet')
df = table.to_pandas()
print(df.head())
```

---

## Troubleshooting

| Issue | Cause | Solution |
|-------|-------|----------|
| `ModuleNotFoundError: No module named 'kalshifolder'` | Not running from project root with `-m` | `cd KalshiMonorepo && python -m kalshifolder.websocket.streamer` |
| `RuntimeError: Missing PROD_KEYID or PROD_KEYFILE` | `.env` not loaded or credentials missing | Set `PROD_KEYID` and `PROD_KEYFILE` in `.env` |
| ClickHouse connection refused | Network/firewall issue | Check `CLICKHOUSE_URL` is reachable; `nc -zv localhost 8123` |
| Parquet files growing too large | Compression ratio estimate off | Reduce `max_bytes` parameter or increase `compression_ratio` estimate |
| Memory usage increasing | Buffers not flushing | Check ClickHouse is accepting inserts; verify network; increase `BATCH_ROWS` |

---

## Summary

This system is a **production-ready, real-time market data ingestion pipeline** that:
- Connects to Kalshi's live WebSocket feed
- Maintains local orderbook state
- Writes hot data to ClickHouse for queries
- Archives cold data to Parquet for storage
- Scales to multiple markets and high-frequency updates

For cloud deployment, ensure ClickHouse is running, credentials are configured, and the application can access both Kalshi's API and your ClickHouse instance.
