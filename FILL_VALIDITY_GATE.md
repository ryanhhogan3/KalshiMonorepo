# Step 1 & 2: Fill Validity Gate + get_fills() Handling

## Problem
MM fills table was being poisoned with invalid rows:
- `price_cents = 0` (should be [1, 99])
- Missing `exchange_order_id`
- Missing timestamp from API payload
- `size = 0` (already handled, but needs validation)

## Solution Implemented

### 1. Hard Validity Gate in `_normalize_fill()`

Added four validation checks that return `None` (skip the fill) if:

```python
1. size <= 0
   → Skips fills with zero or negative contracts

2. price_cents < 1 or price_cents > 99
   → Skips fills with invalid prices (e.g., 0, 100+)

3. not exchange_order_id
   → Skips fills missing order ID (can't reconcile)

4. not ts (from payload)
   → Skips fills without API-provided timestamp
   → Won't use now() as fallback anymore
```

Each check logs a warning with specific reason for debugging:
```python
logger.warning(json_msg({
    "event": "fill_skipped_invalid_price",
    "exchange_order_id": exch_id,
    "price_cents": price_cents,
}))
```

### 2. Fixed get_fills() Handling

Updated `fetch_and_apply_fills()` to:

```python
# Handle dict responses from API
if isinstance(fills, dict):
    fills = fills.get("fills") or fills.get("data") or fills.get("items") or []

# Ensure it's a list
if isinstance(fills, str) or not isinstance(fills, list):
    fills = []

# Verify all items are dicts
fills = [f for f in fills if isinstance(f, dict)]
```

This prevents accidentally treating an open_orders response as fills.

### 3. Enhanced Logging

Added visibility:
```python
logger.info(json_msg({
    "event": "fills_ingested",
    "count": len(rows),
    "with_decision_id": sum(...),
    "with_side": sum(...),
}))
```

## Before & After

**Before (invalid fills written):**
```
mm_fills:
  price_cents=0 | size=0.0 | exchange_order_id=NULL | ts=NOW()
  price_cents=0 | size=0.0 | exchange_order_id=NULL | ts=NOW()
  → Garbage data pollution
```

**After (invalid fills dropped):**
```
fill_skipped_invalid_price: price_cents=0
fill_skipped_missing_order_id: (no exch_id)
fill_skipped_missing_timestamp: (no ts in payload)
→ Only valid fills written
```

## Verification Steps

### Step 1: Check current state (before deploy)
```bash
docker exec clickhouse clickhouse-client -u default --password default_password --database kalshi -q "
SELECT
  count() AS n,
  sum(price_cents=0) AS zero_price,
  round(100 * zero_price / greatest(n,1), 2) AS zero_rate_pct
FROM kalshi.mm_fills
WHERE ts > now() - INTERVAL 24 HOUR;"
```

Expected result: `zero_rate_pct ~= 100` (most current fills are invalid)

### Step 2: Deploy and monitor logs
```bash
cd ~/apps/KalshiMonorepo
git add -A && git commit -m "Add fill validity gate: drop invalid price/order_id/timestamp"
docker compose -f docker-compose.yml -f docker-compose.mm.yml up -d --build kalshi_mm_engine
```

### Step 3: Watch for skipped fills (should see many)
```bash
docker logs -f kalshi_mm_engine | grep "fill_skipped"
```

Expected to see:
```
fill_skipped_invalid_price: price_cents=0
fill_skipped_missing_order_id: ...
fill_skipped_missing_timestamp: ...
```

### Step 4: Check mm_fills after new fills arrive (should be cleaner)
```bash
docker exec clickhouse clickhouse-client -u default --password default_password --database kalshi -q "
SELECT
  count() AS n,
  sum(price_cents=0) AS zero_price,
  round(100 * zero_price / greatest(n,1), 2) AS zero_rate_pct,
  count(distinct exchange_order_id) AS unique_orders
FROM kalshi.mm_fills
WHERE ts > now() - INTERVAL 1 HOUR;"
```

Expected:
- `zero_rate_pct` drops to 0% (no more invalid prices)
- `unique_orders` > 0 (real fills being inserted)
- `n` = number of real valid fills only

## Files Modified

**src/kalshifolder/mm/providers/reconciliation.py:**
- Enhanced `_normalize_fill()` with four hard validity gates
- Improved `fetch_and_apply_fills()` to handle dict/list API responses
- Added detailed skip logging for each failure reason
- Enhanced success logging with decision_id + side stats

## Key Design Decisions

1. **Return None instead of raising** - Allows graceful filtering of bad fills
2. **Price_cents range [1, 99]** - Valid Kalshi contract prices (0.01 to 0.99)
3. **Require API-provided timestamp** - No fallback to now() to prevent timestamp drift
4. **Log but don't warn on size=0** - Use debug level to avoid noise, already filtered
5. **Separate logs for each validation** - Easier to debug which validation is failing

## Next Steps

After deploy:
1. Monitor `docker logs` for which validations are most commonly triggered
2. If seeing "fill_skipped_*" logs, that's good (catching bad data)
3. Once new valid fills appear in mm_fills, Option C fill ingestion is working correctly
4. Then can investigate why API is sending invalid fills (bad side field? missing count? etc.)
