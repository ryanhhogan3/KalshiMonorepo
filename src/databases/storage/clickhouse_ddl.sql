CREATE DATABASE IF NOT EXISTS kalshi;

CREATE TABLE IF NOT EXISTS kalshi.orderbook_events
(
  type Enum8('snapshot'=1,'delta'=2) CODEC(LZ4),
  sid UInt32 CODEC(Delta, ZSTD(3)),
  seq UInt64 CODEC(Delta, ZSTD(3)),
  market_id UUID CODEC(ZSTD(5)),
  market_ticker LowCardinality(String) CODEC(ZSTD(5)),
  side Enum8('yes'=1,'no'=2) CODEC(LZ4),
  price UInt16 CODEC(DoubleDelta, ZSTD(3)), -- cents
  qty Int32 CODEC(Delta, ZSTD(3)),          -- snapshot: absolute; delta: signed change
  ts DateTime64(6,'UTC') CODEC(DoubleDelta, ZSTD(3)),
  ingest_ts DateTime64(6,'UTC') CODEC(DoubleDelta, ZSTD(3))
)
ENGINE MergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY (market_id, side, price, ts, seq)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS kalshi.latest_levels
(
  market_id UUID,
  market_ticker LowCardinality(String),
  side Enum8('yes'=1,'no'=2),
  price UInt16,
  size Int32,
  ts DateTime64(6,'UTC'),
  ingest_ts DateTime64(6,'UTC')
)
ENGINE ReplacingMergeTree(ts)
ORDER BY (market_id, side, price);
