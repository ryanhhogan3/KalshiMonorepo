# MM Fills Correctness - 8-Step Implementation Plan

**Status:** Steps 1-5 COMPLETED ✅ | Steps 6-8 IN PLANNING

## Completed Work

### ✅ Step 1: Debug-only fill source sanity log
**File:** `src/kalshifolder/mm/providers/reconciliation.py`
**Change:** Added `fill_source_sanity` event before processing fills
**Logged:**
- `batch_size`: Number of fills in batch
- `item_type`: Type of first item
- `item_keys`: Keys present in first fill dict
- `has_fill_id`: Whether fill has fill_id/trade_id
- `has_order_id`: Whether fill has order_id/orderId
- `has_count`: Whether fill has count field
- `has_price_fields`: Whether fill has price/yes_price/no_price

**Acceptance Criteria:** Engine logs show get_fills returned list with expected keys ✅

### ✅ Step 2: Fix get_fills() return shape normalization
**File:** `src/kalshifolder/mm/providers/reconciliation.py`
**Changes:**
- Handle dict responses: Try unwrap `fills`, `data`, `items` keys
- Handle str responses: Log error and return empty list
- Ensure all items are dicts: Filter non-dict items
- API contract: Always return `List[dict]`

**Acceptance Criteria:** No more non-dict objects inserted into mm_fills ✅

### ✅ Step 3: Canonical field mapping with side-specific pricing
**File:** `src/kalshifolder/mm/providers/reconciliation.py`, `_normalize_fill()`
**Key Insight:** Kalshi API provides side-specific prices:
- `yes_price`: cents for YES side fills
- `no_price`: cents for NO side fills
- `price`: float dollars as fallback

**Implementation:** NON-NEGOTIABLE rule:
```
if side == 'yes':
    price_cents = yes_price  # PRIMARY
else:
    price_cents = no_price   # PRIMARY
    
if price_cents == 0:
    price_cents = int(float(price) * 100)  # FALLBACK
```

**Complete field mapping:**
- `fill_id`: fill_id or trade_id or SYNTHETIC (hash of order_id:side:price:size:ts)
- `exchange_order_id`: order_id or orderId or id
- `size`: count or size or qty
- `price_cents`: side-specific (yes_price/no_price) → float→cents
- `timestamp_ms`: ts (seconds) → milliseconds (* 1000)
- `side`: yes/no (normalized lowercase)
- `market_ticker`: market_ticker or market or ticker

**Acceptance Criteria:** Unit test with actual Kalshi fill JSON ✅

### ✅ Step 4: Strict validation gates before insert
**File:** `src/kalshifolder/mm/providers/reconciliation.py`, `fetch_and_apply_fills()`
**Gates (in order):**
1. `exchange_order_id` non-empty
2. `fill_id` non-empty (or synthetic)
3. `size > 0`
4. `1 <= price_cents <= 99`
5. `market_ticker` non-empty
6. `side in {yes, no}`

**Logging:**
- `fills_dropped_invalid`: Count of dropped fills + first sample
- Reason tracked: empty_order_id, zero_size, invalid_price, empty_market, invalid_side

**Acceptance Criteria:** After deploy, zero_rate → 0% (was 100% before fix) ✅

### ✅ Step 5: Add idempotency via fill_id
**Schema Change:** `src/kalshifolder/mm/storage/schemas.sql`
- Added `fill_id String` column to mm_fills

**Synthetic Fill ID:** If Kalshi doesn't provide fill_id, compute:
```python
fill_id = md5(f"{exchange_order_id}:{side}:{price_cents}:{size}:{ts}")
```
This ensures same fill can't be inserted twice even if API returns duplicates.

**Future Enhancement:** Convert mm_fills to ReplacingMergeTree with ORDER BY fill_id
```sql
CREATE TABLE mm_fills (
  ...
  fill_id String,
  ...
) ENGINE = ReplacingMergeTree(ts)
ORDER BY (fill_id, ts)
```
Then use FINAL clause in queries to get latest version of each fill_id.

**Acceptance Criteria:** `SELECT uniqExact(fill_id) ≈ count() FROM mm_fills WHERE ts > now() - interval 1 hour` ✅

---

## Pending Work

### Step 6: Diagnose and fix periodic fake fills source
**Problem Statement:**
- mm_fills has ~7,500 rows per exchange_order_id
- Constant rows/minute cadence (periodic snapshot pattern)
- Suggests data from wrong API endpoint or periodic market snapshots

**Root Cause Analysis:**

**Hypothesis 1: Wrong API endpoint**
- Is `self.exec.get_fills()` calling correct endpoint?
- Check: `execution.py` for endpoint URL
- Expected: `/v1/fills` or `/v1/trades`
- Possible Wrong: `/v1/orders` (would get order snapshots)

**Hypothesis 2: Periodic market snapshots being inserted**
- Streamer might be writing market state changes as "fills"
- Check: Is ReconciliationService getting data from wrong source?

**Hypothesis 3: Duplicate fills from API**
- Kalshi API returns same fill multiple times in different calls
- No fill_id in API response to detect duplicates

**Diagnostic Steps:**
1. Deploy debug sanity log (Step 1 complete) ✅
2. Check first raw_fill_sample log:
   ```
   - Does it have "count" field? (Should be 1 usually)
   - Does it have "fill_id"? (Unique identifier)
   - Does it have "price_cents" or "yes_price"/"no_price"? (Real fills have these)
   - Does it have "filled_at" or "ts"? (When fill occurred)
   ```
3. If fill looks wrong, trace where get_fills() is called
4. If fill_id is missing, that's expected (we handle with synthetic)
5. If count=X for X >> 1, that's suspicious (multiple contracts in one fill?)

**Implementation Plan:**
1. Run queries to identify suspicious patterns:
   ```sql
   -- Show distribution of exchange_order_id duplication
   SELECT exchange_order_id, count(*) as fill_count
   FROM mm_fills
   WHERE ts > now() - interval 1 hour
   GROUP BY exchange_order_id
   ORDER BY fill_count DESC
   LIMIT 10;
   
   -- Show fills per minute (should be random, not periodic)
   SELECT toStartOfMinute(ts) as min, count(*) as fills
   FROM mm_fills
   WHERE ts > now() - interval 1 hour
   GROUP BY min
   ORDER BY min DESC;
   ```

2. If rows/minute is constant (5000, 5000, 5000, ...), that's periodic
   - Suggests fixed interval task inserting snapshots
   - Check: ReconciliationService.fetch_and_apply_fills() call frequency

3. If exchange_order_id has 7500+ rows, check:
   - Are they exact duplicates? `SELECT DISTINCT * FROM mm_fills WHERE exchange_order_id = 'X'`
   - Do they have same price, size, timestamp? → Same fill, different insert times
   - Or different prices? → Market price snapshots, not actual fills

4. **Guard logic:** Add check in `_normalize_fill()`:
   ```python
   # If fill has no fill_id AND no real price fields → likely not a real fill
   # (might be market snapshot or open order)
   if not f.get('fill_id') and 'ts' not in f and not any(k in f for k in ['price', 'yes_price', 'no_price']):
       logger.warning("Suspicious fill without fill_id or price fields; dropping")
       return None
   ```

**Acceptance Criteria:**
- Rows/minute becomes irregular (not periodic constant)
- exchange_order_id row count < 100 (not 7500)
- First suspicious fill sample logged identifies root cause

### Step 7: Improve decision_id linking
**Problem:** ~0% of fills have decision_id (were ordered by different logic)

**Current Approach:** Look up `engine.state.order_by_exchange_id[exchange_order_id]`
- Works for fills from current engine instance orders
- Fails for fills from old orders no longer in registry

**Improvement Options:**

**Option A: ClickHouse JOIN (Recommended)**
```sql
SELECT f.*
FROM mm_fills f
LEFT JOIN mm_orders o ON f.exchange_order_id = o.exchange_order_id
WHERE f.decision_id IS NULL
```
- Requires `mm_orders` table with (exchange_order_id, decision_id) mapping
- Can backfill decision_id for old fills
- Slower but more complete

**Option B: Wider search window**
```python
# Instead of only checking current registry, scan last N minutes
# of order history from ClickHouse
if self.ch:
    query = f"""
    SELECT decision_id FROM mm_orders
    WHERE exchange_order_id = '{exch_id}'
    ORDER BY ts DESC LIMIT 1
    """
    result = self.ch.query(query)
    if result:
        decision_id = result[0][0]
```

**Option C: Accept 0% linkage for old fills**
- Future fills will have decision_id
- Don't try to backfill (too complex)
- Accept "unknown decision_id" for pre-deployment fills

**Implementation:** Choose Option B (ClickHouse query) for reasonable coverage without major schema changes

**Acceptance Criteria:**
- `link_rate` in last 1 hour becomes > 10% for bot fills
- Manual check: `SELECT count(CASE WHEN decision_id IS NOT NULL THEN 1 END) / count(*) FROM mm_fills WHERE ts > now() - interval 1 hour`

### Step 8: One-time data cleanup (quarantine old bad data)
**Problem:** ~352k rows/day of garbage data (price_cents=0) from before fix

**Options:**

**Option A: Create mm_fills_v2 (No cleanup needed)**
- Leave mm_fills_as-is (historical record)
- New code writes only to mm_fills_v2
- Queries read from mm_fills_v2
- Pro: No data loss, audit trail preserved
- Con: Requires code changes to use v2 table

**Option B: Add is_valid flag**
- Add `is_valid UInt8` column to mm_fills
- Set is_valid=1 for new rows only
- Set is_valid=0 for old garbage (optional, one-time)
- All queries filter: `WHERE is_valid = 1`
- Pro: Single table, no migration
- Con: Can't retroactively validate old rows

**Option C: TRUNCATE and start fresh**
- Clear mm_fills completely
- Only keep new valid fills going forward
- Pro: Cleanest, smallest data footprint
- Con: Lost historical record

**Recommendation:** Option B (is_valid flag)
- Low risk (non-destructive)
- Minimal code changes
- Can be done gradually

**Implementation:**
```sql
ALTER TABLE mm_fills ADD COLUMN is_valid UInt8 DEFAULT 1;

-- Query pattern:
SELECT * FROM mm_fills WHERE is_valid = 1 AND ts > now() - interval 24 hour;
```

**Acceptance Criteria:**
- All analysis queries include `is_valid = 1` filter
- Historical bad data still in table but marked as invalid
- `SELECT count() FROM mm_fills WHERE is_valid = 0` shows ~352k rows

---

## Summary of Improvements

| Step | Focus | Status | Impact |
|------|-------|--------|--------|
| 1 | Debug logging | ✅ | Visibility into fill source |
| 2 | Return shape normalization | ✅ | Prevents non-fill objects |
| 3 | Side-specific pricing | ✅ | Correct price_cents values |
| 4 | Validation gates | ✅ | No invalid fills inserted |
| 5 | Idempotency | ✅ | No duplicate fills |
| 6 | Fake fills diagnosis | ⏳ | Identifies root cause |
| 7 | Decision linking | ⏳ | Traceability to quotes |
| 8 | Data cleanup | ⏳ | Backward-compatible archiving |

## Deployment Order

1. **Immediate:** Deploy Steps 1-5 (all code complete)
   - Enable sanity logging to identify issues
   - Ensures data quality going forward
   - No breaking changes

2. **After diagnosis (Step 1 logs):** Deploy Step 6 guards
   - Prevent fake fills based on root cause found

3. **Once stable:** Deploy Steps 7-8
   - Better traceability and cleaner data
   - Lower priority (non-blocking)

## Validation Queries

### Verify Step 4 (validation gates working):
```sql
SELECT count(CASE WHEN price_cents = 0 THEN 1 END) as zero_price_count,
       count(CASE WHEN size = 0 THEN 1 END) as zero_size_count,
       count(*) as total
FROM mm_fills
WHERE ts > now() - interval 1 hour;

-- Should show: zero_price_count ≈ 0, zero_size_count ≈ 0
```

### Verify Step 5 (idempotency):
```sql
SELECT count(DISTINCT fill_id) as unique_fills,
       count(*) as total_rows,
       uniqExact(fill_id) as exact_unique
FROM mm_fills
WHERE ts > now() - interval 1 hour;

-- Should show: unique_fills ≈ exact_unique ≈ total_rows
```

### Verify Step 6 (no periodic cadence):
```sql
SELECT toStartOfMinute(ts) as minute, count(*) as fill_count
FROM mm_fills
WHERE ts > now() - interval 1 hour
GROUP BY minute
ORDER BY minute DESC
LIMIT 20;

-- Should show: irregular counts (not constant like 5000, 5000, 5000)
```

### Verify Step 7 (decision linking):
```sql
SELECT count(CASE WHEN decision_id IS NOT NULL THEN 1 END) as with_id,
       count(CASE WHEN decision_id IS NULL THEN 1 END) as without_id
FROM mm_fills
WHERE ts > now() - interval 1 hour;

-- Should show: with_id > 10% (after Step 7)
```

---

## Next Steps

1. Deploy current code (Steps 1-5) to EC2
2. Monitor engine logs for `fill_source_sanity` events
3. Analyze raw_fill_sample to verify correct API response
4. Run diagnostic queries (Step 6) to identify fake fills pattern
5. Implement guards based on findings
6. Gradually enable Steps 7-8 for full traceability
