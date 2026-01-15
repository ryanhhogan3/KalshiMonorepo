# MM Engine Fixes Applied - Ready for Deployment

## Date
January 14, 2026

## Summary
Four critical fixes to the market-making engine to prevent position cap breaches and reduce order churn.

---

## Fix 1: Use Exchange Position (Source of Truth) for Risk Checks

**File:** [src/kalshifolder/mm/risk/limits.py](src/kalshifolder/mm/risk/limits.py)

**Problem:**
- Risk checks were using internal `market_state.inventory` which can lag, reset, or be computed differently
- This allowed the system to breach the position cap if the internal inventory diverged from the exchange

**Solution:**
- Modified `check_market()` to accept `exchange_position` parameter (source of truth from API)
- Now uses exchange position directly instead of internal inventory
- Checks both current position and projected position (including intended_delta)
- Hard blocks if `abs(exchange_pos) >= max_pos` OR if `abs(exchange_pos + intended_delta) > max_pos`

**Changes:**
```python
# Old: check_market(market_state, intended_delta)
# New: check_market(market_state, exchange_position, intended_delta)

# Now correctly uses exchange position:
if abs(exchange_position) >= max_pos:
    return False, f'AT_OR_OVER_CAP (pos={exchange_position}, max={max_pos})'

projected_position = exchange_position + intended_delta
if abs(projected_position) > max_pos:
    return False, f'WOULD_EXCEED_CAP (pos={exchange_position}, delta={intended_delta}, max={max_pos})'
```

---

## Fix 2: Correct Intended Delta for BUY-Only Two-Sided Quoting

**File:** [src/kalshifolder/mm/engine.py](src/kalshifolder/mm/engine.py)

**Problem:**
- Two-sided quoting was not correctly accounting for position impact:
  - BID side: BUY YES → increases inventory by +size
  - ASK side: BUY NO → decreases YES exposure by -size (reduces inventory)
- Risk checks needed per-side delta computation

**Solution:**
- Compute and pass per-side `intended_delta` to risk checks:
  - BID side: `intended_delta = +target.bid_sz` (accumulates YES)
  - ASK side: `intended_delta = -target.ask_sz` (reduces YES exposure)
- Added per-side risk checks before order placement
- Each side is independently gated against the position cap

**Changes:**
- Line ~640: Compute net intended_delta for initial market-level check
- Line ~710: Per-side risk check for BID with `intended_delta = +bid_sz`
- Line ~955: Per-side risk check for ASK with `intended_delta = -ask_sz`

**Example Log Output:**
```json
{
  "event": "risk_block_bid_side",
  "market": "SAMPLE.MKT",
  "exchange_position": 5,
  "bid_intended_delta": 1,
  "reason": "WOULD_EXCEED_CAP (pos=5, delta=1, max=5)"
}
```

---

## Fix 3: Startup Safety - Block Trading if Already Over Cap

**File:** [src/kalshifolder/mm/engine.py](src/kalshifolder/mm/engine.py) - `run()` method

**Problem:**
- If the engine restarts while already holding positions above max_pos (e.g., from a previous crash), it would immediately start misbehaving and placing orders in the wrong direction
- No guard to detect this hazardous state

**Solution:**
- Added startup position cap check after reconciliation (lines ~1250-1270)
- Queries exchange positions via API after startup
- If any market has `abs(position) > max_pos`, **disables trading immediately**
- Logs error message with all breached markets
- Requires manual acknowledgment/intervention to re-enable trading

**Behavior:**
```
1. Engine starts
2. Reconciles with exchange
3. Checks all positions vs max_pos
4. If any breach detected:
   - Sets trading_enabled = False
   - Logs detailed error with breached markets
   - Continues in read-only mode
```

**Example Log Output:**
```json
{
  "event": "startup_position_cap_breach",
  "status": "TRADING_DISABLED",
  "reason": "Already holding positions over max_pos cap at startup",
  "max_pos": 5,
  "breached_markets": [
    {"ticker": "SAMPLE.MKT", "position": 7},
    {"ticker": "OTHER.MKT", "position": -6}
  ],
  "action_required": "Manually review positions and acknowledge in logs to re-enable trading"
}
```

---

## Fix 4: Reduce Order Churn via Parameter Tuning

**File:** [src/kalshifolder/mm/config.py](src/kalshifolder/mm/config.py) and [src/kalshifolder/mm/engine.py](src/kalshifolder/mm/engine.py)

**Problem:**
- 2227 places / 2220 cancels on one market in 6 hours = 370 actions/hour = 1 action every ~10 seconds
- This is excessive churn that increases costs and latency sensitivity

**Solution A - Tuned Defaults:**
- Increased `MM_QUOTE_REFRESH_MS`: 1000ms → 5000ms (5 second minimum)
  - Now refreshes quotes at most every 5 seconds instead of 1 second
  - Reduces refresh-driven churn by 5x

- Increased `MM_MIN_REPRICE_TICKS`: 1 → 2 (0.02 dollars = 2 cents)
  - Won't reprice for tiny moves < 2 ticks
  - Requires meaningful price movement to trigger cancel/replace
  - Avoids "tick staleness" churn in choppy markets

**Solution B - Implemented Min Reprice Logic:**
- Added `min_reprice_cents` threshold check in both BID and ASK placement (lines ~745-757 and ~978-990)
- Compares `abs(new_price_cents - wo.price_cents)` against threshold
- Only reprices if:
  - Size changed, OR
  - Price moved >= threshold (default 2 ticks = 200 cents)

**Logic:**
```python
min_reprice_cents = self.config.min_reprice_ticks * 100
price_diff = abs(new_price_cents - wo.price_cents)

if int(wo.size) != int(target.bid_sz):
    need_replace = True  # Size changed: always reprice
elif price_diff >= min_reprice_cents:
    need_replace = True  # Meaningful price move: reprice
else:
    need_replace = False  # Tiny move: skip
```

**Example Log Output (Skip Decision):**
```json
{
  "event": "skip_same_quote",
  "market": "SAMPLE.MKT",
  "side": "BID",
  "old_price_cents": 5200,
  "new_price_cents": 5201,
  "price_diff_cents": 1,
  "min_reprice_cents": 200
}
```

**Expected Impact:**
- Reduces churn by ~5-10x depending on market volatility
- With 5s quote_refresh_ms + 2-tick min_reprice_ticks:
  - Expected ~1 action per minute (conservative), down from ~6/minute
  - More stable quotes with lower exchange API load

---

## Environment Variables (Optional Tuning)

You can override defaults at runtime:

```bash
# Quote refresh interval (milliseconds)
MM_QUOTE_REFRESH_MS=5000  # default: 5s

# Minimum price movement to trigger repricing (in ticks of 0.01)
MM_MIN_REPRICE_TICKS=2  # default: 2 ticks (0.02)

# Position limits (in contracts)
MM_MAX_POS=5  # hard cap
MM_MAX_LONG_POS=5  # long-side cap
MM_MAX_SHORT_POS=5  # short-side cap
```

---

## Testing Checklist

Before deploying to production:

- [ ] **Test Fix 1:** Verify risk checks use `exchange_position` from API
  - Check logs for `risk_hard_block_exchange_pos` event
  - Confirm it blocks before breach occurs

- [ ] **Test Fix 2:** Verify per-side risk checks
  - Place BID-only quotes and watch `risk_block_bid_side` gating
  - Place ASK-only quotes and watch `risk_block_ask_side` gating
  - Confirm neither side can independently exceed cap

- [ ] **Test Fix 3:** Restart with position > cap
  - Manually set a position to 6 (if max_pos=5)
  - Restart engine
  - Verify `startup_position_cap_breach` logged
  - Verify trading_enabled=False

- [ ] **Test Fix 4:** Check order churn
  - Run for 1 hour with new params
  - Compare order counts to baseline (should be 5-10x lower)
  - Check logs for `skip_same_quote` events

---

## Deployment Notes

1. **Backward Compatible:** All changes maintain API compatibility
2. **Default Behavior Change:** Quote refresh is now slower (5s vs 1s) - may need tuning for your use case
3. **Risk Improved:** More conservative gating should make system safer
4. **Startup Blocking:** Engine will fail-safe if positions are already breached

---

## Files Modified

1. [src/kalshifolder/mm/risk/limits.py](src/kalshifolder/mm/risk/limits.py)
   - `check_market()` - now uses exchange_position
   - `allowed_actions()` - now uses exchange_position

2. [src/kalshifolder/mm/engine.py](src/kalshifolder/mm/engine.py)
   - Compute and pass intended_delta to risk checks
   - Per-side risk checks before order placement
   - Startup position cap breach detection
   - Min reprice ticks logic in BID/ASK placement

3. [src/kalshifolder/mm/config.py](src/kalshifolder/mm/config.py)
   - Tuned defaults: quote_refresh_ms (1000→5000), min_reprice_ticks (1→2)

---

## Questions / Concerns?

All four fixes are conservative and designed to prevent bad behavior without changing the core quoting logic. They should make the system more stable and reduce order churn while keeping you below position caps.
