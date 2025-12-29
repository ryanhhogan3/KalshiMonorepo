-- DDL for market-making engine tables (enhanced with required fields)
CREATE TABLE IF NOT EXISTS mm_decisions (
  ts DateTime64(3,'UTC'),
  decision_id UUID,
  engine_instance_id String,
  engine_version String,
  market_ticker String,
  bb_px Float64,
  ba_px Float64,
  mid Float64,
  spread Float64,
  fair Float64,
  target_bid_px Float64,
  target_ask_px Float64,
  target_bid_sz Float64,
  target_ask_sz Float64,
  inv_before Float64,
  inv_after_est Float64,
  reason_codes Array(String),
  params_json String
) ENGINE = MergeTree()
PARTITION BY toDate(ts)
ORDER BY (market_ticker, ts);

CREATE TABLE IF NOT EXISTS mm_order_actions (
  ts DateTime64(3,'UTC'),
  action_id UUID,
  decision_id UUID,
  engine_instance_id String,
  engine_version String,
  market_ticker String,
  client_order_id String,
  action_type LowCardinality(String),
  side LowCardinality(String),
  api_side LowCardinality(String),
  price Float64,
  price_cents UInt16,
  size Float64,
  replace_of_client_order_id String,
  request_json String
) ENGINE = MergeTree()
PARTITION BY toDate(ts)
ORDER BY (market_ticker, ts);

CREATE TABLE IF NOT EXISTS mm_order_responses (
  ts DateTime64(3,'UTC'),
  action_id UUID,
  engine_instance_id String,
  engine_version String,
  market_ticker String,
  client_order_id String,
  status LowCardinality(String),
  exchange_order_id String,
  reject_reason String,
  latency_ms UInt32,
  response_json String
) ENGINE = MergeTree()
PARTITION BY toDate(ts)
ORDER BY (market_ticker, ts);

CREATE TABLE IF NOT EXISTS mm_fills (
  ts DateTime64(3,'UTC'),
  engine_instance_id String,
  engine_version String,
  market_ticker String,
  exchange_order_id String,
  client_order_id String,
  side LowCardinality(String),
  price Float64,
  price_cents UInt16,
  size Float64,
  fee Float64,
  decision_id UUID,
  raw_json String
) ENGINE = MergeTree()
PARTITION BY toDate(ts)
ORDER BY (market_ticker, ts);

CREATE TABLE IF NOT EXISTS mm_positions (
  ts DateTime64(3,'UTC'),
  engine_instance_id String,
  engine_version String,
  market_ticker String,
  position Float64,
  avg_cost Float64,
  realized_pnl Float64,
  unrealized_pnl Float64,
  source LowCardinality(String)
) ENGINE = MergeTree()
PARTITION BY toDate(ts)
ORDER BY (market_ticker, ts);

CREATE TABLE IF NOT EXISTS mm_heartbeat (
  ts DateTime64(3,'UTC'),
  engine_instance_id String,
  engine_version String,
  markets UInt32,
  quote_rate_per_min Float64,
  actions_per_min Float64,
  rejects_per_min Float64,
  open_orders UInt32,
  stale_markets UInt32,
  kill_switch UInt8,
  notes String
) ENGINE = MergeTree()
PARTITION BY toDate(ts)
ORDER BY (ts, engine_instance_id);
