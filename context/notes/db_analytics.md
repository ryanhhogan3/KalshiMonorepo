1. Schema Sanity Check — Do the tables exist?
docker exec -it clickhouse clickhouse-client --format PrettyCompact -q "
  SHOW TABLES FROM kalshi
"


Expected:

orderbook_events
latest_levels


If not, the schema bootstrap failed.

2. Are all markets receiving any events?

This checks ingestion per ticker:

docker exec -it clickhouse clickhouse-client --format PrettyCompact -q "
  SELECT
    market_ticker,
    count(*) AS rows,
    min(ts) AS first_ts,
    max(ts) AS last_ts
  FROM kalshi.orderbook_events
  GROUP BY market_ticker
  ORDER BY market_ticker
"


Interpretation:

Column	Meaning
rows	Total rows (1 snapshot often = 40–200 rows)
first_ts	First seen event time
last_ts	Last seen event time

If all 20 tickers appear, ingestion works.

3. Snapshot vs Delta Breakdown (Is the market active?)
docker exec -it clickhouse clickhouse-client --format PrettyCompact -q "
  SELECT
    market_ticker,
    type,
    count(*) AS rows
  FROM kalshi.orderbook_events
  GROUP BY market_ticker, type
  ORDER BY market_ticker, type
"


Interpretation:

• snapshot rows = initial book
• delta rows = real-time changes

Active markets will have deltas.
Quiet markets will only have snapshots.

4. Deep Dive Into a Single Market (recent 50 events)

Replace ticker as needed:

docker exec -it clickhouse clickhouse-client --format Vertical -q "
  SELECT
    type,
    side,
    price,
    qty,
    ts,
    ingest_ts
  FROM kalshi.orderbook_events
  WHERE market_ticker = 'INDIACLIMATE-30'
  ORDER BY ts DESC
  LIMIT 50
"


What you want to see:

Mix of snapshot and delta

ts should be Kalshi event time

ingest_ts should be your EC2 machine's current UTC time

5. Confirm Latest Levels Table (Your Order Book State)
docker exec -it clickhouse clickhouse-client --format PrettyCompact -q "
  SELECT
    market_ticker,
    side,
    price,
    size,
    ts AS last_event
  FROM kalshi.latest_levels
  ORDER BY market_ticker, side, price
  LIMIT 200
"


Each ticker should have many rows (all price levels present).

If any ticker is missing, it means:

snapshot wasn't processed, or

upsert_latest failed

6. "Last Activity" Leaderboard (which markets are active)
docker exec -it clickhouse clickhouse-client --format PrettyCompact -q "
  SELECT
    market_ticker,
    max(ts) AS last_ts,
    count(*) AS rows
  FROM kalshi.orderbook_events
  GROUP BY market_ticker
  ORDER BY last_ts DESC
"


This shows:

Most recently updated markets first

Markets with no activity will cluster at bottom

7. Count Snapshot Events vs Delta Events Properly (exploded snapshots)

Useful for debugging sequence issues:

docker exec -it clickhouse clickhouse-client --format PrettyCompact -q "
  SELECT
    market_ticker,
    sum(type='snapshot') AS snapshot_rows,
    sum(type='delta') AS delta_rows,
    count(*) AS total
  FROM kalshi.orderbook_events
  GROUP BY market_ticker
  ORDER BY total DESC
"

8. Detect Tickers With No New Data in the Last X Minutes

Example: last 10 minutes:

docker exec -it clickhouse clickhouse-client --format PrettyCompact -q "
  SELECT
    market_ticker,
    max(ts) AS last_ts,
    now() - max(ts) AS age
  FROM kalshi.orderbook_events
  GROUP BY market_ticker
  ORDER BY age DESC
"


Look for age > 5 minutes during market hours.

9. Show Only Deltas (Use this to debug real-time changes)
docker exec -it clickhouse clickhouse-client --format PrettyCompact -q "
  SELECT
    market_ticker,
    side,
    price,
    qty,
    ts
  FROM kalshi.orderbook_events
  WHERE type='delta'
  ORDER BY ts DESC
  LIMIT 100
"


If this table never updates → you’re not receiving deltas.

10. Disk Footprint — Are your tables growing correctly?
docker exec -it clickhouse clickhouse-client --format PrettyCompact -q "
  SELECT
    table,
    formatReadableSize(sum(bytes_on_disk)) AS disk_usage,
    count() AS active_parts
  FROM system.parts
  WHERE database = 'kalshi' AND active
  GROUP BY table
"


You should see steady growth but nothing insane.

🔥 BONUS: One Command to Rule Them All

This gives ticker, snapshot count, delta count, last event time:

docker exec -it clickhouse clickhouse-client --format PrettyCompact -q "
  SELECT
    market_ticker,
    sum(type='snapshot') AS snaps,
    sum(type='delta') AS deltas,
    max(ts) AS last_event
  FROM kalshi.orderbook_events
  GROUP BY market_ticker
  ORDER BY last_event DESC
"

11. Count all Snapshots as singular event to compare them with deltas

docker exec -it clickhouse clickhouse-client --format PrettyCompact -q "SELECT market_ticker, uniqExactIf((sid, seq), type='snapshot') AS snapshot_events, uniqExactIf((sid, seq), type='delta') AS delta_events, round(snapshot_events / greatest(delta_events, 1), 3) AS snaps_per_delta_event, max(ts) AS last_ts FROM kalshi.orderbook_events GROUP BY market_ticker ORDER BY snaps_per_delta_event DESC;"

12. Ensure no missing tickers for data within CH in the last 30 minutes

docker exec -it clickhouse clickhouse-client --format PrettyCompact -q "
SELECT
  countIf(market_id = toUUID('00000000-0000-0000-0000-000000000000')) AS nil_market_id_rows,
  count() AS rows,
  round(nil_market_id_rows / rows, 6) AS frac_nil
FROM kalshi.orderbook_events
WHERE ts >= now() - INTERVAL 30 MINUTE;
"


