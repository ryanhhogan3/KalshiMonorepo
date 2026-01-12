# CRITICAL FIXES: Fill Deduplication & Timestamp Bug

**Problem Diagnosis**: Your sample showed 427 rows in 1 hour with only 55 unique fill_ids - the same fills being re-inserted every 10 seconds.

**Root Causes**: 
1. **Timestamp bug**: Using `now()` instead of fill's actual time → each insert looks "new"
2. **Missing fill_id**: Some rows have blank fill_id → can't dedupe
3. **No idempotency**: No check before insert if fill already exists in CH

---

## Fix 1: ALWAYS Generate fill_id (Never Blank)

**File**: `src/kalshifolder/mm/providers/reconciliation.py` → `_normalize_fill()`

**Change**: Generate synthetic fill_id immediately if not in API response

```python
# 2. Extract and validate fill_id - MUST ALWAYS HAVE A VALUE
fill_id = f.get('fill_id') or f.get('trade_id')
if not fill_id:
    # CRITICAL: Generate synthetic fill_id immediately
    # Ensures every fill has stable, deterministic ID for deduping
    import hashlib
    fill_tuple = f"{exchange_order_id}:{side}:{f.get('count', 0)}:{f.get('yes_price', f.get('no_price', f.get('price', 0)))}:{f.get('ts', 0)}"
    fill_id = hashlib.md5(fill_tuple.encode()).hexdigest()

# Ensure fill_id is never blank
if not str(fill_id).strip():
    logger.warning(...)
    return None
```

**Acceptance**: `SELECT count() AS n, sum(fill_id='') AS blank FROM mm_fills WHERE ts > now() - INTERVAL 1 HOUR`
- Expected: `blank = 0` (no empty fill_ids)

---

## Fix 2: Use Fill's Actual Timestamp (Not now())

**File**: `src/kalshifolder/mm/providers/reconciliation.py` → `fetch_and_apply_fills()`

**Change**: Convert fill's ts (milliseconds) to DateTime string

```python
# CRITICAL FIX: Use fill's actual timestamp, not now()
from datetime import datetime, timezone
fill_ts_ms = f.get('ts', 0)
if fill_ts_ms > 0:
    fill_ts_sec = fill_ts_ms / 1000.0 if fill_ts_ms > 1e11 else fill_ts_ms
    try:
        fill_dt = datetime.fromtimestamp(fill_ts_sec, tz=timezone.utc)
        ts_str = fill_dt.strftime('%Y-%m-%d %H:%M:%S')
    except:
        ts_str = time.strftime('%Y-%m-%d %H:%M:%S')
else:
    ts_str = time.strftime('%Y-%m-%d %H:%M:%S')

rows.append({
    'ts': ts_str,  # ← Now using fill's actual time, not now()
    ...
})
```

**Acceptance**: Engine logs show timestamp verification
```json
{
  "event": "fill_pipeline_stats",
  "first_fill_ts_iso": "2026-01-11T23:52:12Z",
  "now_iso": "2026-01-11T23:52:45Z",
  "ts_age_sec": 33
}
```

Also verify in CH:
```sql
SELECT min(ts) as min_ts, max(ts) as max_ts 
FROM kalshi.mm_fills 
WHERE ts > now() - INTERVAL 10 MINUTE;
```

Expected: `min_ts` and `max_ts` should be from the past 10 minutes (real fill times), not all clustered at current time.

---

## Fix 3: Idempotency Filter (Query CH Before Insert)

**File**: `src/kalshifolder/mm/providers/reconciliation.py` → `fetch_and_apply_fills()`

**Change**: Before inserting rows, check if fill_ids already exist in CH

```python
# CRITICAL: Idempotency filter - don't re-insert fills that exist
existing_fill_ids = set()
if rows:
    incoming_fill_ids = [r['fill_id'] for r in rows if r.get('fill_id')]
    if incoming_fill_ids:
        try:
            # Chunk query (max 500 IDs per query)
            for i in range(0, len(incoming_fill_ids), 500):
                chunk = incoming_fill_ids[i:i+500]
                id_list = ', '.join([f"'{fid}'" for fid in chunk])
                result = self.ch._exec_and_read(
                    f"SELECT DISTINCT fill_id FROM kalshi.mm_fills WHERE fill_id IN ({id_list})"
                )
                if result:
                    existing_fill_ids.update(line.strip() for line in result.strip().split('\n'))
        except Exception as e:
            logger.warning({"error": str(e)})
            # If check fails, insert anyway (better duplicate than missing fill)

# Filter to only new fills
new_rows = [r for r in rows if r.get('fill_id') not in existing_fill_ids]

# Insert only new rows
if new_rows:
    self.ch.insert('mm_fills', new_rows)
    logger.info({"event": "fills_ingested", "count": len(new_rows), ...})
```

**Acceptance**: Logs show deduping working
```json
{
  "event": "fills_deduped",
  "total_rows": 427,
  "new_rows": 5,
  "duplicates_filtered": 422
}
```

Then query CH to verify:
```sql
SELECT uniqExact(fill_id) as unique_ids, count() as total_rows
FROM kalshi.mm_fills
WHERE ts > now() - INTERVAL 1 HOUR;
```

Expected: `unique_ids ≈ total_rows` (not 55 unique with 427 total)

---

## How This Fixes the 1000x Problem

**Before**:
```
Tick 1 (10:52:10): Get 5 fills from API
  → Insert 5 rows with ts=now() (10:52:30)

Tick 2 (10:52:20): Get same 5 fills from API
  → Insert 5 rows with ts=now() (10:52:45) ← looks different!

Tick 3 (10:52:30): Get same 5 fills from API
  → Insert 5 rows with ts=now() (10:52:55) ← looks different!

After 1 hour: 5 fills/hour API, 300+ rows in CH (60 ticks × 5)
fill_id sometimes blank, sometimes filled (inconsistent)
```

**After**:
```
Tick 1 (10:52:10): Get 5 fills from API
  → Normalize: 5 fills with fill_id, ts=2026-01-11T23:52:12Z
  → Query CH: existing_ids = {} (empty)
  → Insert: 5 new rows

Tick 2 (10:52:20): Get same 5 fills from API
  → Normalize: 5 fills with fill_id, ts=2026-01-11T23:52:12Z (SAME ts!)
  → Query CH: existing_ids = {fill_id_1, fill_id_2, fill_id_3, fill_id_4, fill_id_5}
  → Filter: 0 new rows
  → Insert: nothing (deduped)

Tick 3 (10:52:30): Get same 5 fills from API
  → Normalize: 5 fills with fill_id, ts=2026-01-11T23:52:12Z (SAME ts!)
  → Query CH: existing_ids = {all 5}
  → Filter: 0 new rows
  → Insert: nothing (deduped)

After 1 hour: 5 rows in CH (exactly matching API)
All rows have valid fill_id, correct original timestamp
```

---

## Deployment Steps

```bash
# Push code
cd ~/apps/KalshiMonorepo
git add -A
git commit -m "Fix fill dedup: timestamp bug + idempotency filter"
git push

# On EC2
git pull
docker compose down
docker compose -f docker-compose.yml -f docker-compose.mm.yml up -d --build --force-recreate

# Monitor
docker logs -f kalshi_mm_engine | grep -E "fill_pipeline_stats|fills_deduped|fills_ingested"
```

---

## Validation Queries (Run After Deployment)

### 1. Verify timestamp fix
```sql
SELECT 
  count(*) as total,
  min(ts) as oldest_fill,
  max(ts) as newest_fill,
  dateDiff('minute', min(ts), max(ts)) as time_span_minutes
FROM kalshi.mm_fills
WHERE ts > now() - INTERVAL 1 HOUR;
```

Expected: `time_span_minutes > 1` (spread over time, not all at same second)

### 2. Verify fill_id always populated
```sql
SELECT 
  count() as total,
  sum(fill_id = '') as blank_ids,
  sum(fill_id IS NULL) as null_ids
FROM kalshi.mm_fills
WHERE ts > now() - INTERVAL 1 HOUR;
```

Expected: `blank_ids = 0`, `null_ids = 0`

### 3. Verify idempotency working
```sql
SELECT 
  uniqExact(fill_id) as unique_fills,
  count() as total_rows,
  round(100.0 * uniqExact(fill_id) / count(), 1) as uniqueness_pct
FROM kalshi.mm_fills
WHERE ts > now() - INTERVAL 1 HOUR;
```

Expected: `uniqueness_pct ≈ 100%` (was ~13% before: 55 unique / 427 total)

### 4. Verify volume matches API
```sql
SELECT count() as fills_last_hour
FROM kalshi.mm_fills
WHERE ts > now() - INTERVAL 1 HOUR;
```

Expected: ~5-10 (API volume), NOT 4,774

### 5. Check dedup logs
```bash
docker logs kalshi_mm_engine --tail 100 | grep fills_deduped
```

Expected: See events like `{"event": "fills_deduped", "total_rows": 5, "new_rows": 0, "duplicates_filtered": 5}`

---

## Success Criteria

✅ All of these must be true:

1. **Timestamp spread**: min(ts) to max(ts) > 1 minute (not clustered at now())
2. **No blank fill_ids**: sum(fill_id='') = 0 in last hour
3. **Uniqueness**: uniqExact(fill_id) / count() > 99% (not 13%)
4. **Volume matches API**: CH fills/hour ≈ API fills/hour
5. **Logs show dedup**: Logs show fills_deduped event with >90% filtered as duplicates
6. **Idempotency working**: After first hour, each new tick shows 0 new rows if API returns same fills

---

## If Still Broken

1. Check engine logs for errors in idempotency check
2. Verify `_exec_and_read` method exists in ch_writer.py (it does, added earlier)
3. If dedup query times out, reduce chunk size from 500 to 100
4. Check ClickHouse logs: `docker logs clickhouse | tail -50`

Last resort: `TRUNCATE TABLE kalshi.mm_fills` and restart engine to get clean data only.
