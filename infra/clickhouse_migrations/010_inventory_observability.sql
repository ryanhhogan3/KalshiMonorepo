-- Adds explicit inventory/observability columns for mm_decisions and a
-- dedicated position snapshot table so ClickHouse queries can explain risk.

ALTER TABLE kalshi.mm_decisions
    ADD COLUMN IF NOT EXISTS pos_filled Float64 DEFAULT 0 AFTER inv_after_est;

ALTER TABLE kalshi.mm_decisions
    ADD COLUMN IF NOT EXISTS pos_open_exposure Float64 DEFAULT 0 AFTER pos_filled;

ALTER TABLE kalshi.mm_decisions
    ADD COLUMN IF NOT EXISTS pos_total_est Float64 DEFAULT 0 AFTER pos_open_exposure;

ALTER TABLE kalshi.mm_decisions
    ADD COLUMN IF NOT EXISTS allowed UInt8 DEFAULT 1 AFTER pos_total_est;

ALTER TABLE kalshi.mm_decisions
    ADD COLUMN IF NOT EXISTS block_stage LowCardinality(String) DEFAULT '' AFTER allowed;

ALTER TABLE kalshi.mm_decisions
    ADD COLUMN IF NOT EXISTS block_codes Array(LowCardinality(String)) DEFAULT [] AFTER block_stage;

ALTER TABLE kalshi.mm_decisions
    ADD COLUMN IF NOT EXISTS inv_convention LowCardinality(String) DEFAULT '' AFTER block_codes;

ALTER TABLE kalshi.mm_decisions
    ADD COLUMN IF NOT EXISTS reason_codes Array(String) DEFAULT [] AFTER inv_convention;

ALTER TABLE kalshi.mm_decisions
    ADD COLUMN IF NOT EXISTS open_orders_count UInt32 DEFAULT 0 AFTER reason_codes;

ALTER TABLE kalshi.mm_decisions
    ADD COLUMN IF NOT EXISTS open_yes_bid_size Float64 DEFAULT 0 AFTER open_orders_count;

ALTER TABLE kalshi.mm_decisions
    ADD COLUMN IF NOT EXISTS open_yes_ask_size Float64 DEFAULT 0 AFTER open_yes_bid_size;

ALTER TABLE kalshi.mm_decisions
    ADD COLUMN IF NOT EXISTS open_no_bid_size Float64 DEFAULT 0 AFTER open_yes_ask_size;

ALTER TABLE kalshi.mm_decisions
    ADD COLUMN IF NOT EXISTS open_no_ask_size Float64 DEFAULT 0 AFTER open_no_bid_size;

CREATE TABLE IF NOT EXISTS kalshi.mm_position_snapshots (
    ts DateTime64(3,'UTC'),
    engine_instance_id String,
    engine_version String,
    market_ticker String,
    pos_filled Float64,
    avg_cost Float64,
    source LowCardinality(String),
    raw_json String
) ENGINE = MergeTree()
PARTITION BY toDate(ts)
ORDER BY (market_ticker, ts);
