Below is a **Copilot-ready TODO** (written like an implementation plan) that targets the *real* root cause: **`latest_levels` is not a merge-safe “latest state” source**, and your current query is also logically wrong for BBO.

I’m going to be blunt: your current `get_batch_best_bid_ask()`:

* Treats `maxIf(price, side='yes')` as “best bid” → that’s actually “highest YES price across all levels”, but **not necessarily the best bid unless you filter `size>0` and use “latest per level” semantics**.
* Uses `argMaxIf(size, price, side='yes')` → that returns the `size` at the **highest price**, not the size at the *best bid level after deduping by ingest_ts*.
* Uses `max(ts)` which can drift/stale because `ReplacingMergeTree` doesn’t guarantee dedupe without `FINAL` or `argMax(..., ingest_ts)`.

So: we’ll fix both **the query semantics** and **the underlying “latestness” guarantee**.

---

## Copilot TODO: Fix `latest_levels` correctness + make MM reads merge-safe

### 0) Establish the intended semantics

* `orderbook_events`: append-only truth.
* `latest_levels`: “current state” per `(market, side, price)` where **size=0 means cleared level**.
* **MM needs BBO**: best YES bid and best YES ask (or derived from NO), computed from the *latest state*.

---

## A) Fix ClickHouse schema / engine for `latest_levels`

### A1) Use `ingest_ts` as the version column (not `ts`)

**Problem:** exchange `ts` is not a stable “latest” version in resnap / gap scenarios.
**Fix:** ensure `latest_levels` is:

* `ENGINE = ReplacingMergeTree(ingest_ts)`
* `ORDER BY (market_ticker, side, price)` (or `(market_id, side, price)` + include ticker if you query by ticker a lot)

**TODO:**

1. Add a migration script (or bootstrap step) to:

   * Create `latest_levels_v2` with correct engine.
   * Backfill from existing `latest_levels`.
   * Swap tables (rename) with minimal downtime.
2. Alternatively, if you can afford a “drop + recreate” in dev: drop and recreate.

**Backfill query idea:**

* Insert deduped “latest per key” into v2 using `argMax` by `ingest_ts`:

```sql
INSERT INTO latest_levels_v2
SELECT
  market_id,
  market_ticker,
  side,
  price,
  argMax(size, ingest_ts) AS size,
  argMax(ts, ingest_ts) AS ts,
  max(ingest_ts) AS ingest_ts
FROM latest_levels
GROUP BY market_id, market_ticker, side, price;
```

### A2) Optional: add a projection or view for merge-independent reads

If you keep ReplacingMergeTree, **reads must not depend on background merges**.

**TODO:**

* Create a view (or materialized view) that exposes deduped levels via `argMax(size, ingest_ts)` grouped by key.
* Example view: `latest_levels_deduped`.

---

## B) Fix streamer writes to `latest_levels`

### B1) Ensure *every* delta/snapshot path updates `latest_levels` consistently

**Problem:** your data shows `orderbook_events` fresh but some `latest_levels` tickers stale → code path gaps.

**TODO:**

1. Audit all `ch.upsert_latest()` call sites (you said 3).
2. Ensure for every processed book update that changes size at a level:

   * Write `size` (including 0 on deletes)
   * Write correct `market_ticker`, `side`, `price`, `ts`, `ingest_ts`.
3. Add an invariant test/log:

   * If you wrote N events for ticker in last minute, you should see at least some latest writes for ticker unless literally no levels changed.

### B2) Fix `upsert_latest` inputs: `ts` vs `ingest_ts`

**TODO:**

* Ensure `ingest_ts` is always `now_utc()` at ingestion time.
* Ensure `ts` is the exchange timestamp from message.
* Don’t reuse old `ingest_ts` anywhere.

### B3) Add lightweight periodic “latest_levels freshness” metric

**TODO:**

* In streamer heartbeat loop, query ClickHouse (or use internal counters) for:

  * `max(ingest_ts)` by ticker in `latest_levels` for subscribed tickers
  * compare to `max(ts)` in `orderbook_events` per ticker
* Log a single line like:

  * `latest_levels_lag_s_by_ticker={...}` for top 5 worst

---

## C) Fix MM query: compute BBO from merge-independent deduped state

### C1) Replace current query with “dedupe per (ticker, side, price) via argMax(size, ingest_ts)”

**Problem:** without dedupe, `maxIf` can pick stale versions.

**TODO:**

* Change `get_batch_best_bid_ask()` to use a subquery:

**Correct approach:**

1. Build deduped levels:

```sql
WITH lv AS (
  SELECT
    market_ticker,
    side,
    price,
    argMax(size, ingest_ts) AS size,
    max(ingest_ts) AS ingest_ts
  FROM kalshi.latest_levels
  WHERE market_ticker IN (...)
  GROUP BY market_ticker, side, price
)
SELECT
  market_ticker,
  max(ingest_ts) AS ingest_ts,
  maxIf(price, side='yes' AND size > 0) AS yes_best_bid_px,
  argMaxIf(size, price, side='yes' AND size > 0) AS yes_best_bid_sz,
  minIf(price, side='yes' AND size > 0) AS yes_best_ask_px,
  argMinIf(size, price, side='yes' AND size > 0) AS yes_best_ask_sz,

  -- If you still want NO-side “bid/ask”:
  maxIf(price, side='no' AND size > 0) AS no_best_bid_px,
  argMaxIf(size, price, side='no' AND size > 0) AS no_best_bid_sz,
  minIf(price, side='no' AND size > 0) AS no_best_ask_px,
  argMinIf(size, price, side='no' AND size > 0) AS no_best_ask_sz
FROM lv
GROUP BY market_ticker
FORMAT JSONEachRow;
```

2. In Python, decide how to derive YES ask from NO bid, etc.

   * If your MM logic quotes YES, then you probably want `yes_best_bid` and `yes_best_ask` directly.
   * Only derive from NO if your feed is asymmetric.

### C2) Fix timestamp returned to MM

**Problem:** you use `max(ts)` which is exchange timestamp and can be stale or missing; what you actually want for “stale market age” is **ingest freshness**.

**TODO:**

* Return both:

  * `ingest_ts_ms` from `max(ingest_ts)`
  * `exchange_ts_ms` from `argMax(ts, ingest_ts)` if needed
* Use `ingest_ts_ms` for stale detection in the engine.

### C3) Expand return fields to include both sides cleanly

**TODO:**

* Return:

  * `yes_bb_px`, `yes_bb_sz`, `yes_ba_px`, `yes_ba_sz`
  * `no_bb_px`, `no_bb_sz`, `no_ba_px`, `no_ba_sz`
  * `ingest_ts_ms`, `exchange_ts_ms`
* Stop mapping `ba_px = (100 - no_px)/100` unless you’re 100% sure that’s how Kalshi book corresponds for your quote instrument.

---

## D) Add a tiny `latest_bbo` table (optional but strongly recommended)

### D1) Create `latest_bbo` keyed by `market_ticker`

**Goal:** MM reads one row per market. Zero heavy grouping.

**TODO:**

* Streamer maintains BBO as it updates the in-memory orderbook and writes to:

  * `latest_bbo(market_ticker, yes_bid_px, yes_bid_sz, yes_ask_px, yes_ask_sz, ts, ingest_ts)`
* Engine reads `latest_bbo` instead of grouping `latest_levels` every tick.

---

## E) Add verification scripts (so you can prove it’s fixed)

### E1) “Mismatch detector” query (events fresh but levels stale)

**TODO:**
Write a ClickHouse query/script that outputs top laggards:

```sql
SELECT
  e.market_ticker,
  max(e.ts) AS events_max,
  max(l.ingest_ts) AS levels_ingest_max,
  dateDiff('second', levels_ingest_max, now()) AS levels_age_s,
  dateDiff('second', max(l.ingest_ts), max(e.ts)) AS lag_s
FROM orderbook_events e
LEFT JOIN latest_levels l
  ON l.market_ticker = e.market_ticker
WHERE e.ts > now() - INTERVAL 2 HOUR
GROUP BY e.market_ticker
ORDER BY levels_age_s DESC
LIMIT 50;
```

### E2) MM provider unit test with synthetic duplicated levels

**TODO:**

* Insert multiple rows for same `(ticker, side, price)` with different `ingest_ts` and sizes.
* Assert `get_batch_best_bid_ask()` returns the latest size and correct best prices.

---

## F) Quick config tuning (after correctness is fixed)

**TODO:**

* Split “stale” thresholds:

  * `MM_MAX_LEVEL_AGE_MS` should be based on **ingest_ts**, not exchange ts.
  * Set it per market category (fast crypto vs slow politics).
* Example defaults:

  * fast markets: 5–10s
  * slow markets: 5–15 minutes

---

# Immediate “drop-in” Copilot task list (short form)

1. Update `latest_levels` engine to `ReplacingMergeTree(ingest_ts)` and align ORDER BY to query pattern.
2. Make MM query merge-independent using `argMax(size, ingest_ts)` grouped by `(market_ticker, side, price)` in a subquery.
3. Compute true YES best bid/ask using `maxIf/minIf` with `size>0`; fix bid/ask size selection accordingly.
4. Return `ingest_ts_ms` and use it for stale checks.
5. Add streamer instrumentation: per ticker, log `events_max_ts` vs `levels_max_ingest_ts` lag.
6. Audit all `upsert_latest` call sites to ensure all update paths write `latest_levels`.
7. Add a tiny `latest_bbo` table as the “MM read surface” (optional but best).

---

If you want, I can also hand you the **exact rewritten `get_batch_best_bid_ask()` Python function** (drop-in replacement) using the merge-safe SQL above, with clean parsing + both timestamps.
