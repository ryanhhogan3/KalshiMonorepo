# Option C: Fill Ingestion Fix

## Problem
MM fills were being written to ClickHouse with `size=0`, even though the Kalshi UI showed real 1-contract fills. This was a field mapping issue.

## Root Cause
The `_normalize_fill()` method in `ReconciliationService` was looking for a `size` field in the raw Kalshi API response, but Kalshi's API uses a `count` field to represent the number of contracts filled.

## Solution Implemented

### 1. Fixed Field Mapping (size/count)
**File:** `src/kalshifolder/mm/providers/reconciliation.py`

Updated `_normalize_fill()` to:
```python
# Extract size/count from fill
# Kalshi API uses 'count' for number of contracts
size = f.get('count') or f.get('size') or f.get('qty') or 0
```

Also added **zero-size filtering**:
```python
# Skip fills with 0 or negative size
if size <= 0:
    logger.debug(json_msg({"event": "fill_skipped_zero_size", ...}))
    return None
```

This prevents garbage fills (size=0) from being written to ClickHouse.

### 2. Added Debug Logging
When fills are received, the first fill JSON structure is logged for validation:
```python
logger.info(json_msg({"event": "raw_fill_sample", "fill": fills[0], "fill_keys": list(fills[0].keys())}))
```

This helps verify that the Kalshi API is returning the expected fields.

### 3. Added decision_id Linking
Uses the engine's order registries to link fills back to their quotes:
```python
# Look up decision_id from engine's order registries
decision_id = None
if self.engine:
    exch_id = f.get('exchange_order_id')
    if exch_id and exch_id in self.engine.state.order_by_exchange_id:
        order_ref = self.engine.state.order_by_exchange_id[exch_id]
        decision_id = order_ref.decision_id
```

This populates the `decision_id` field in mm_fills, enabling traceability from quote → order → fill.

### 4. Enhanced ClickHouse Insert
Now writes all correct fields to mm_fills:
```python
rows.append({
    'ts': time.strftime('%Y-%m-%d %H:%M:%S'),
    'engine_instance_id': engine_instance_id,
    'engine_version': engine_version,
    'market_ticker': f['market_ticker'],
    'exchange_order_id': f['exchange_order_id'],
    'client_order_id': f['client_order_id'],
    'side': f['side'],
    'price_cents': f['price_cents'],
    'size': f['size'],  # Now correctly mapped from 'count'
    'decision_id': decision_id,
    'raw_json': str(f['raw']),
})
```

Logs success with decision_id count:
```python
logger.info(json_msg({"event": "fills_ingested", "count": len(rows), "with_decision_id": sum(1 for r in rows if r.get('decision_id'))}))
```

## Expected Result After Deployment

When MM engine places and fills orders:

1. **ClickHouse logs (old behavior):**
   ```
   SELECT market_ticker, size, decision_id FROM mm_fills LIMIT 5;
   KXHIGHDEN-26JAN10-B45.5 | 0.0 | NULL   ← Bad!
   ```

2. **ClickHouse logs (new behavior):**
   ```
   SELECT market_ticker, size, decision_id FROM mm_fills LIMIT 5;
   KXHIGHDEN-26JAN10-B45.5 | 1.0 | 7c3a... ← Good!
   ```

3. **Engine logs:**
   ```
   {"event": "raw_fill_sample", "fill": {...}, "fill_keys": ["id", "count", "price", ...]}
   {"event": "fills_ingested", "count": 2, "with_decision_id": 2}
   ```

## Testing

To verify the fix is working:

1. **Check for fills with actual size:**
   ```bash
   docker exec clickhouse clickhouse-client -u default --password default_password --database kalshi -q \
   "SELECT market_ticker, exchange_order_id, size, decision_id FROM mm_fills WHERE size > 0 LIMIT 5;"
   ```

2. **Check raw fill sample in logs:**
   ```bash
   docker logs kalshi_mm_engine | grep "raw_fill_sample"
   ```

3. **Verify decision_id linking:**
   ```bash
   docker logs kalshi_mm_engine | grep "fills_ingested"
   ```

## Files Modified
- `src/kalshifolder/mm/providers/reconciliation.py`
  - Added json_msg import
  - Updated `_normalize_fill()` to use 'count' field
  - Added zero-size filtering
  - Enhanced fill ingestion with decision_id lookup
  - Added debug logging

## Deployment Notes
No schema changes required—mm_fills table already has all necessary fields. Just deploy and the new logic will automatically:
- Map fills correctly from 'count' → 'size'
- Skip zero-size fills
- Link fills to quotes via decision_id
