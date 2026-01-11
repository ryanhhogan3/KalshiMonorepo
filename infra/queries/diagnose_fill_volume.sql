-- DIAGNOSTIC QUERIES: Identify source of 1000x volume mismatch
-- Run these on ClickHouse to understand why CH has 4,774 fills/hour while API says 5 fills/hour

-- 1. VOLUME ANALYSIS
-- Show fills per minute (should be irregular if real, not periodic)
SELECT 
    toStartOfMinute(ts) as minute, 
    count(*) as fill_count,
    uniqExact(exchange_order_id) as unique_orders
FROM kalshi.mm_fills
WHERE ts > now() - INTERVAL 1 HOUR
GROUP BY minute
ORDER BY minute DESC;

-- Expected: Irregular counts (random), not constant like 6, 6, 6, 6
-- If all rows are ~6 per minute → periodic snapshot, not real fills


-- 2. ROWS PER ORDER ANALYSIS
-- Show how many rows each order has (should be 1, maybe 2 if reprocess)
SELECT 
    exchange_order_id,
    count(*) as row_count,
    min(ts) as first_ts,
    max(ts) as last_ts,
    dateDiff('second', min(ts), max(ts)) as time_span_sec
FROM kalshi.mm_fills
WHERE ts > now() - INTERVAL 24 HOUR
GROUP BY exchange_order_id
ORDER BY row_count DESC
LIMIT 20;

-- Expected: Most orders have row_count = 1
-- Actual: Shows ~6600+ for some orders → duplicated inserts


-- 3. PRICE DISTRIBUTION
-- Check if prices are really being validated
SELECT 
    count(*) as total,
    sum(price_cents = 0) as zero_price,
    sum(price_cents BETWEEN 1 AND 99) as valid_price,
    max(price_cents) as max_price,
    min(price_cents) as min_price,
    round(100.0 * sum(price_cents BETWEEN 1 AND 99) / count(), 2) as valid_pct
FROM kalshi.mm_fills
WHERE ts > now() - INTERVAL 1 HOUR;

-- Expected (after fix): valid_pct ≈ 100%, zero_price ≈ 0
-- Actual: Still showing 92% zero_price → validation not working


-- 4. FILL_ID UNIQUENESS
-- Check if fill_id column exists and if values are unique
SELECT 
    count(*) as total_rows,
    uniqExact(fill_id) as unique_fill_ids,
    sum(fill_id IS NULL) as null_fill_ids,
    round(100.0 * uniqExact(fill_id) / count(), 2) as uniqueness_pct
FROM kalshi.mm_fills
WHERE ts > now() - INTERVAL 1 HOUR;

-- Expected: uniqueness_pct ≈ 100% (one row per real fill)
-- Actual: Unknown if column exists (ERROR if it doesn't)


-- 5. DECISION_ID COVERAGE
-- Check what % of fills are linked to real engine decisions
SELECT 
    count(*) as total,
    sum(decision_id IS NOT NULL) as with_decision,
    sum(decision_id IS NULL) as without_decision,
    round(100.0 * sum(decision_id IS NOT NULL) / count(), 2) as link_pct
FROM kalshi.mm_fills
WHERE ts > now() - INTERVAL 1 HOUR;

-- Expected: link_pct > 10% for bot fills
-- Actual: Shows ~2.37% → most fills aren't from bot orders


-- 6. SIDE DISTRIBUTION
-- Check if sides are correctly parsed
SELECT 
    side,
    count(*) as count,
    sum(price_cents = 0) as zero_price
FROM kalshi.mm_fills
WHERE ts > now() - INTERVAL 1 HOUR
GROUP BY side;

-- Expected: Both 'yes' and 'no' have valid prices
-- Actual: Should show if one side is consistently zero-priced


-- 7. CHRONOLOGICAL PATTERN
-- Show when most fills occurred (reveals if periodic)
SELECT 
    toStartOfFiveMinutes(ts) as five_min_bucket,
    count(*) as fill_count
FROM kalshi.mm_fills
WHERE ts > now() - INTERVAL 12 HOUR
GROUP BY five_min_bucket
ORDER BY five_min_bucket DESC
LIMIT 50;

-- Expected: Varied counts (trading activity is random)
-- Actual: Likely shows periodic spikes (e.g., every minute)


-- 8. SCHEMA CHECK
-- Verify mm_fills table structure
DESCRIBE TABLE kalshi.mm_fills;

-- Expected to see: fill_id String column present
-- Actual: Might be missing if schema migration didn't run


-- 9. COMPARISON: NEW FILLS vs OLD (POISONED) DATA
-- Assuming new code wrote after a certain time, compare
-- Set @cutoff = timestamp when fix was deployed
SELECT 
    count(*) as total,
    sum(price_cents = 0) as zero_price,
    sum(price_cents BETWEEN 1 AND 99) as valid_price,
    count(DISTINCT exchange_order_id) as unique_orders
FROM kalshi.mm_fills
WHERE ts > now() - INTERVAL 1 HOUR;  -- After fix

-- vs

SELECT 
    count(*) as total,
    sum(price_cents = 0) as zero_price,
    sum(price_cents BETWEEN 1 AND 99) as valid_price,
    count(DISTINCT exchange_order_id) as unique_orders
FROM kalshi.mm_fills
WHERE ts BETWEEN (now() - INTERVAL 25 HOUR) AND (now() - INTERVAL 24 HOUR);  -- Before fix

-- If new code is working: Before has ~100% zero_price, After has ~0% zero_price
-- Actual: Before AND After both ~92% zero → code not deployed or not used


-- 10. POSSIBLE SOURCE: Open Orders bleeding through
-- If we're somehow writing open orders as fills, they would lack certain fields
-- Group by which fills have the LEAST common real-fill fields
SELECT 
    count(*) as count,
    sum(fill_id IS NOT NULL) as has_fill_id,
    sum(decision_id IS NOT NULL) as has_decision,
    max(price_cents) as max_price
FROM kalshi.mm_fills
WHERE ts > now() - INTERVAL 1 HOUR
GROUP BY 
    (fill_id IS NOT NULL),
    (decision_id IS NOT NULL)
LIMIT 10;

-- If there's a group with fill_id IS NULL AND decision_id IS NULL, 
-- those are likely not real fills (or synthetic fills from our code)
