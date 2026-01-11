# DEPLOYMENT GUIDE: Fix Fill Volume Mismatch (API 5 → CH 4,774)

**Status**: Ready to deploy to EC2

**Key Changes**:
1. Schema migrations now apply ALTER TABLE for missing columns
2. Reconciliation now has "real fill" predicate to drop non-fills
3. Detailed logging at each stage (API → Raw → Real → Valid → Inserted)

---

## Step 1: Deploy Code to EC2

```bash
cd ~/apps/KalshiMonorepo

# Create backup branch
git add -A
git commit -m "EC2 state before fill volume fix"
git branch ec2-backup-$(date +%Y%m%d)
git push -u origin ec2-backup-$(date +%Y%m%d)

# Pull latest
git switch main
git fetch origin
git reset --hard origin/main

# Rebuild and restart
docker compose down
docker compose -f docker-compose.yml -f docker-compose.mm.yml up -d --build --force-recreate
```

---

## Step 2: Verify Schema Migration Ran

Once MM engine starts, check logs for schema verification:

```bash
# Watch engine logs for schema output
docker logs -f kalshi_mm_engine | grep -E "mm_fills|columns"

# Expected to see something like:
# "mm_fills columns: ['ts', 'engine_instance_id', ..., 'fill_id', ...]"
```

Also verify in ClickHouse:

```bash
# Directly check table schema
docker exec -it clickhouse clickhouse-client --query "DESCRIBE TABLE kalshi.mm_fills" --user default --password default_password --database kalshi

# Expected to see fill_id String column present
```

---

## Step 3: Monitor Fill Pipeline on First Run

The engine will now log detailed stats at each stage. Watch for these events:

```bash
docker logs -f kalshi_mm_engine | grep -E "fill_source_sanity|fill_pipeline_stats|fill_predicate_filtered"
```

**Expected logs:**

```json
{
  "event": "fill_source_sanity",
  "n_api": 5,
  "n_raw": 5,
  "item_keys": ["order_id", "count", "price", "yes_price", "no_price", "ts", "market_ticker", "side"],
  "has_fill_id": false,
  "has_order_id": true,
  "has_count": true,
  "has_price_fields": true,
  "has_ts": true
}

{
  "event": "fill_pipeline_stats",
  "n_api": 5,
  "n_raw": 5,
  "n_real": 5,
  "n_valid": 5,
  "n_inserted": 5,
  "predicate_pass_rate": "100.0%",
  "validation_pass_rate": "100.0%",
  "gate_pass_rate": "100.0%"
}
```

**If you see drops**, it means either:
- `n_api != n_raw`: API response is not a list of dicts (wrong endpoint?)
- `n_raw != n_real`: Fills missing critical fields (order_id, ts, count, price)
- `n_real != n_valid`: Price validation failing (price_cents not in 1-99)
- `n_valid != n_inserted`: Final gates failing (shouldn't happen if norm passed)

---

## Step 4: Run Diagnostic Queries

After 30-60 seconds of operation, run these queries:

```bash
# 1. Check volume mismatch (THIS IS THE KEY TEST)
docker exec -it clickhouse clickhouse-client <<'SQL'
SELECT 
    count() AS fill_count,
    uniqExact(exchange_order_id) AS unique_orders,
    sum(price_cents = 0) AS zero_price_count,
    round(100.0 * sum(price_cents > 0) / count(), 2) AS valid_pct
FROM kalshi.mm_fills
WHERE ts > now() - INTERVAL 1 MINUTE;
SQL

# Expected AFTER FIX: ~5 fills, ~5 orders, valid_pct ≈ 100%
# Current BEFORE FIX: ~80 fills, ~50 orders, valid_pct ≈ 0%
```

```bash
# 2. Check if fill_id column exists
docker exec -it clickhouse clickhouse-client <<'SQL'
SELECT fill_id, count() as n FROM kalshi.mm_fills WHERE ts > now() - INTERVAL 1 MINUTE GROUP BY fill_id LIMIT 5;
SQL

# Expected: See fill_id values (hashes or UUIDs)
# If error "Unknown identifier fill_id": Schema migration failed
```

```bash
# 3. Check periodicity pattern
docker exec -it clickhouse clickhouse-client <<'SQL'
SELECT 
    toStartOfMinute(ts) as minute,
    count(*) as fills_per_minute
FROM kalshi.mm_fills
WHERE ts > now() - INTERVAL 10 MINUTE
GROUP BY minute
ORDER BY minute DESC;
SQL

# Expected AFTER FIX: Irregular counts (0, 1, 2, 0, 1, 1, ...)
# Current BEFORE FIX: Constant pattern (6, 6, 6, 6, 6, ...) = periodic
```

```bash
# 4. Check rows per order (should be 1, not 6600+)
docker exec -it clickhouse clickhouse-client <<'SQL'
SELECT 
    count(*) as total_rows,
    max(row_count) as max_rows_per_order,
    count(CASE WHEN row_count = 1 THEN 1 END) as orders_with_1_row,
    count(CASE WHEN row_count > 1 THEN 1 END) as orders_with_duplicates
FROM (
    SELECT exchange_order_id, count(*) as row_count
    FROM kalshi.mm_fills
    WHERE ts > now() - INTERVAL 1 HOUR
    GROUP BY exchange_order_id
);
SQL

# Expected AFTER FIX: max_rows_per_order ≤ 2, orders_with_duplicates ≈ 0
# Current BEFORE FIX: max_rows_per_order ≈ 6600, lots of duplicates
```

---

## Step 5: Identify Root Cause if Still Broken

If volume is still ~1000x too high, investigate:

### A. Check if predicate is filtering fills

```bash
docker logs kalshi_mm_engine --tail 100 | grep fill_predicate_filtered
```

If you see:
```json
{
  "event": "fill_predicate_filtered",
  "n_api": 100,
  "n_raw": 100,
  "n_real": 5,
  "dropped_by_predicate": 95
}
```

That means 95 of the 100 items from API don't have order_id/ts/count/price fields. What ARE they? 
→ Check raw_fill_sample log to see structure of dropped items

### B. Check if API endpoint is wrong

```bash
docker logs kalshi_mm_engine | grep raw_fill_sample -A 5 | head -30
```

Look at the first fill's keys. If you see keys like:
- `open_size`, `original_size` → probably open orders, not fills
- `quote_id` → probably quote events, not fills  
- No `count` or `ts` → definitely not fills

If API is wrong, it's in `src/kalshifolder/mm/providers/execution.py` → `get_fills()` method.
Check what endpoint it's calling:
```bash
grep -n "get_fills\|/fills\|/trades\|/orders" src/kalshifolder/mm/providers/execution.py | head -20
```

### C. Check if code path is even being called

Add a debug flag to see if reconciliation is running:

```bash
docker logs kalshi_mm_engine 2>&1 | grep "fill_source_sanity"
```

If this log never appears, reconciliation.fetch_and_apply_fills() is not being called.
Check engine.run_once() to see if it calls reconciliation service.

---

## Step 6: If Still Broken, Quarantine Old Data

If the issue is mixing old (poisoned) data with new data:

```bash
# Clear mm_fills and start fresh (DESTRUCTIVE)
docker exec -it clickhouse clickhouse-client <<'SQL'
TRUNCATE TABLE kalshi.mm_fills;
SQL

# Then restart engine to populate with NEW data only
docker restart kalshi_mm_engine

# Watch logs and verify in 1 minute with diagnostic query
```

This is safe if you accept losing historical fills. Since current data is 92% garbage anyway, it's not a loss.

---

## Success Criteria

✅ **All of these must be true**:

1. **Volume matches API**: CH fills/hour ≈ API fills/hour (within 2x, not 1000x)
2. **Schema migration ran**: `DESCRIBE TABLE kalshi.mm_fills` shows `fill_id` column
3. **Prices are valid**: `sum(price_cents BETWEEN 1 AND 99) / count() > 95%`
4. **No periodic cadence**: Fills/minute is irregular (0, 1, 3, 0, 2, 1, ...)
5. **No duplicate rows**: `max(rows per order_id) ≤ 2`
6. **Logs show full pipeline**: Each run has fill_pipeline_stats with n_api ≈ n_inserted

---

## Rollback Plan

If things get worse:

```bash
# Switch back to backup
cd ~/apps/KalshiMonorepo
git checkout ec2-backup-$(date +%Y%m%d)
docker compose down
docker compose -f docker-compose.yml -f docker-compose.mm.yml up -d --build

# Verify old behavior returned
docker logs -f kalshi_mm_engine
```

---

## Next Steps After Fix

Once you confirm the volume mismatch is resolved:

1. **Monitor for 24 hours**: Confirm sustained volume ≈ API
2. **Check decision_id linking**: Should improve from 2.37% to > 10%
3. **Plan cleanup**: Decide whether to TRUNCATE old poisoned data or add `is_valid` flag
4. **Test reconciliation**: Run backtest against new mm_fills data

---

## Files Modified

- `src/kalshifolder/mm/storage/ch_writer.py` - Added schema migrations
- `src/kalshifolder/mm/providers/reconciliation.py` - Added "real fill" predicate + detailed logging
- `src/kalshifolder/mm/storage/schemas.sql` - Added fill_id column (idempotent)
- `infra/queries/diagnose_fill_volume.sql` - New diagnostic queries

**No breaking changes** - code is backward compatible, only adds validation.
