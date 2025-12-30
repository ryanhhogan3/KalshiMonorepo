-- Migration: create latest_levels_v2 using ReplacingMergeTree(ingest_ts)
-- 1) Create new table
CREATE TABLE IF NOT EXISTS kalshi.latest_levels_v2 (
  market_id String,
  market_ticker String,
  side String,
  price Int32,
  size Float64,
  ts DateTime64(3),
  ingest_ts DateTime64(3)
) ENGINE = ReplacingMergeTree(ingest_ts)
ORDER BY (market_ticker, side, price);

-- 2) Backfill deduped latest rows into v2 using argMax by ingest_ts
INSERT INTO kalshi.latest_levels_v2
SELECT
  market_id,
  market_ticker,
  side,
  price,
  argMax(size, ingest_ts) AS size,
  argMax(ts, ingest_ts) AS ts,
  max(ingest_ts) AS ingest_ts
FROM kalshi.latest_levels
GROUP BY market_id, market_ticker, side, price;

-- 3) Swap tables with minimal downtime (manual step recommended):
-- RENAME TABLE kalshi.latest_levels TO kalshi.latest_levels_old, kalshi.latest_levels_v2 TO kalshi.latest_levels;
-- Verify before dropping old table.

-- Note: adjust schema types to match your source table exactly if they differ.
