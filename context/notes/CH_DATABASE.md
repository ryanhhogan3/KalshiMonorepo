📘 KalshiMonorepo Database Guide (ClickHouse Hot Storage)

This document explains the full structure, purpose, and internal behavior of the ClickHouse database layer used in the KalshiMonorepo ingestion engine.
It is intended for developers, analysts, and operators who want to understand how real-time Kalshi orderbook data is modeled, stored, queried, and protected inside ClickHouse.

🔷 Overview

The ingestion pipeline writes two types of data into ClickHouse:

1. Historical, append-only event data

Table: orderbook_events
Purpose: Store every raw snapshot+delta message coming from Kalshi’s WebSocket feed.

2. Current, deduplicated latest best levels

Table: latest_levels
Purpose: Maintain only the latest known book state per (market, side, price).
This table is continuously overwritten to reflect the real-time bid/ask levels.

These tables serve different roles:

Table	Purpose	Data Volume	Mutability
orderbook_events	Raw historical truth	Very high	Append-only
latest_levels	Current book state	Small	Constantly overwritten

Both are necessary for a production-grade market-making and backtesting system.

🔵 1. orderbook_events (Raw Historical Orderbook Stream)
📌 Purpose

This table stores every orderbook message from Kalshi's WebSocket API — both snapshot (full state) and delta (incremental change) messages.

This provides:

Complete reconstructability

Replay capability

Backtesting correctness

Auditing and debugging

Data science pipelines

📑 Schema
CREATE TABLE orderbook_events
(
    `type` Enum8('snapshot' = 1, 'delta' = 2),
    `sid` UInt32,
    `seq` UInt64,
    `market_id` UUID,
    `market_ticker` LowCardinality(String),
    `side` Enum8('yes' = 1, 'no' = 2),
    `price` UInt16,
    `qty` Int32,
    `ts` DateTime64(6, 'UTC'),
    `ingest_ts` DateTime64(6, 'UTC')
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY (market_id, side, price, ts, seq)
SETTINGS index_granularity = 8192;

📂 Column Documentation
Column	Meaning
type	Snapshot (1) or delta (2)
sid	Kalshi session ID
seq	Monotonic sequence number (critical for ordering)
market_id	UUID identifier for the market
market_ticker	Human-readable ticker
side	YES/NO book side
price	Price level (0–100)
qty	Quantity (number of contracts)
ts	Exchange timestamp inside the event
ingest_ts	Timestamp of ingestion into our pipeline
📦 Why append-only?

Allows reconstruction of the complete orderbook at any time in the past

Makes backtesting accurate and auditable

Guarantees no data loss

Avoids race conditions from updates

🟣 2. latest_levels (Hot View of the Current Orderbook)
📌 Purpose

This table stores only the current latest bid/ask levels, updated continuously during streaming.

It is your real-time analytics and trade decision table.

Think of it as the materialized view of the orderbook state.

📑 Schema
CREATE TABLE latest_levels
(
    `market_id` UUID,
    `market_ticker` LowCardinality(String),
    `side` Enum8('yes' = 1, 'no' = 2),
    `price` UInt16,
    `size` Int32,
    `ts` DateTime64(6, 'UTC'),
    `ingest_ts` DateTime64(6, 'UTC')
)
ENGINE = ReplacingMergeTree(ingest_ts)
PRIMARY KEY (market_id, side, price);

📂 Column Documentation
Column	Meaning
market_id	UUID
market_ticker	Ticker
side	YES/NO
price	Price bucket
size	Current size (updated whenever a delta arrives)
ts	Exchange timestamp of last update
ingest_ts	When our pipeline last updated this row
🔄 Why ReplacingMergeTree?

Ensures only the latest row per (market_id, side, price) survives merges

Automatically cleans old versions

Fits perfectly for "latest known state" semantics

🔥 How ingestion writes to this table

Your ClickHouseSink.upsert_latest does:

On each update, writes a new row for the same (market, side, price)

ClickHouse later merges old versions away

System always holds the current state

🧠 Why Two Tables Are Necessary
✔ orderbook_events

Ground truth
High volume
Historical replay
Append-only
Supports backtesting and audits

✔ latest_levels

Fast query surface
Low volume
Constantly updates
Supports decision-making algorithms

If you tried to do real-time reads from the historical table, you would:

Scan billions of rows

Recompute orderbook state constantly

Introduce massive latency

By keeping both, you get:

Accuracy (events)

Speed (latest)

Safety (append-only logs)

Flexibility (replayable + real-time)


🧱 Internal Mechanics of ClickHouse
📁 1. Partitions

orderbook_events uses:

PARTITION BY toYYYYMM(ts)


This gives:

Monthly partition pruning

Faster deletes

Efficient I/O

🔨 2. MergeTree Behavior

Data is written into "parts"

Background merges combine them

Broken merges can corrupt parts → why we added health checks

🛡️ Data Integrity, Bootstrap, and Health Checks

Your repo now has two important safety modules:

🔵 1. clickhouse_bootstrap.py

Ensures:

Database exists

Tables exist

Schemas match expected version

Columns are added if missing (e.g., size)

This prevents the “Unrecognized column” errors and guarantees schema correctness at runtime.

🔴 2. clickhouse_health.py

Ensures:

ClickHouse is online

Tables exist

No corruption (broken parts) — if supported

Fails fast if unhealthy

Skips broken-parts check gracefully when system table is unavailable

💬 When health is GOOD

You see:

[CH][health] OK: all tables healthy

💥 When health is BAD

You see:

ClickHouseUnhealthyError: Table orderbook_events has 3 broken parts...


The streamer will refuse to start, preventing corrupted data ingestion.

📊 Query Examples
Get best YES/NO prices
SELECT market_ticker, side, price, size
FROM latest_levels
ORDER BY market_ticker, side, price DESC;

Rebuild a historical book
SELECT *
FROM orderbook_events
WHERE market_id = {id}
ORDER BY seq;

Count deltas per market
SELECT market_ticker, count()
FROM orderbook_events
WHERE type = 'delta'
GROUP BY market_ticker;

🚀 How Backtesting Uses These Tables

orderbook_events provides event-by-event reconstruction of the book

latest_levels provides a quick starting point or fast validation

Combined with Parquet cold storage for older data, you get:

Low-latency recent data

Cheap large-volume historical storage

This hybrid architecture is extremely common in high-frequency trading systems.

🧭 Summary
Component	Purpose	Behavior
orderbook_events	Full historical event log	Append-only
latest_levels	Hot view of current book	Replacing/overwrite style
Bootstrap	Creates & fixes schema	Runs automatically
Health check	Ensures ClickHouse safe before ingestion	Fails fast
Sink	Writes data into both tables efficiently	Column-oriented inserts

This architecture gives you:

Reliability

Speed

Recoverability

Perfect replay

Real-time analytics

Everything your Kalshi market-making and research stack needs.