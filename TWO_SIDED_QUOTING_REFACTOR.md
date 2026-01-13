# Two-Sided BUY-Only Quoting Refactor

## Changes Made ✅

1. **Strategy (simple_mm.py)**
   - ✅ Quote dataclass now returns `yes_bid_px`, `no_bid_px`, `yes_bid_sz`, `no_bid_sz`
   - ✅ NO bid price computed as: `no_bid = 1.0 - yes_bid` (ensures 100¢ constraint)
   - ✅ Backwards-compatible properties for bid_px/ask_px/ask_sz

2. **State Model (models.py)**
   - ✅ Renamed `working_bid` → `working_yes_buy`
   - ✅ Renamed `working_ask` → `working_no_buy`
   - ✅ Both are WorkingOrder objects tracking separate BUY positions

## Changes Needed in engine.py

### 1. Quote evaluation logging (DONE)
Updated to reference target.yes_bid_px, target.no_bid_px, target.yes_bid_sz, target.no_bid_sz

### 2. BID side → YES BUY side (Lines 689-894)

Replace references:
- `mr.working_bid` → `mr.working_yes_buy`
- `mr.last_place_bid_ms` → `mr.last_place_yes_buy_ms`
- `target.bid_px` → `target.yes_bid_px`
- `target.bid_sz` → `target.yes_bid_sz`
- `'BID'` internal_side marker → `'YES_BUY'`
- Logging event names: "throttle_bid" → "throttle_yes_buy", etc.

**Key handler locations in BID block:**
- Line 689: `if target.bid_px is not None:` → `if target.yes_bid_px is not None:`
- Line 696: `target.bid_px` → `target.yes_bid_px`
- Line 702: `target.bid_sz` → `target.yes_bid_sz`
- Line 774, 790, 793, 797: All `target.bid_*` → `target.yes_bid_*`
- Line 788, 800: `side='BID'` → `side='YES_BUY'`
- Line 808: `side="yes"` stays (Kalshi API)
- Line 810: `action="buy"` stays (Kalshi API)

### 3. ASK side → NO BUY side (Lines 900-1090)

**CRITICAL: ASK orders become NO side BUY orders**

Replace:
- `mr.working_ask` → `mr.working_no_buy`
- `mr.last_place_ask_ms` → `mr.last_place_no_buy_ms`
- `target.ask_px` → `target.no_bid_px`
- `target.ask_sz` → `target.no_bid_sz`
- `'ASK'` internal_side marker → `'NO_BUY'`
- `side="yes", action="sell"` → `side="no", action="buy"`

**Key handler locations in ASK block:**
- Line 900: `if target.ask_px is not None:` → `if target.no_bid_px is not None:`
- Line 907: `target.ask_px` → `target.no_bid_px`
- Line 910: `target.ask_sz` → `target.no_bid_sz`
- Line 983: `side='ASK'` → `side='NO_BUY'`
- Line 1003: **CRITICAL** `side="yes", action="sell"` → `side="no", action="buy"`
- Line 1005: `size=int(target.ask_sz)` → `size=int(target.no_bid_sz)`

### 4. Order registry lookups

Lines 800, 883, 995, 1078 reference `internal_side='BID'` or `internal_side='ASK'`:
- Update to `'YES_BUY'` and `'NO_BUY'` respectively

### 5. Startup cleanup (Line 1188)

Already fixed: Uses `self.market_selector.get_active_markets()` ✅

## Changes Needed in reconciliation.py (fill handling)

The fill reconciliation must handle NO side fills:

```python
# In normalize_fill or similar:
# When filling a NO order:
side_yes_no = f.get('side')  # 'no'
action = 'buy'
delta = -size  # BUY NO decreases YES inventory
```

Update the delta logic in lines 650-680:
```python
if action and isinstance(action, str):
    a = action.lower()
    if 'buy' in a:
        if side_yes_no == 'yes':
            delta = size          # BUY YES
        else:
            delta = -size         # BUY NO (equivalent to selling YES)
    elif 'sell' in a:
        if side_yes_no == 'yes':
            delta = -size         # SELL YES
        else:
            delta = size          # SELL NO (equivalent to buying YES)
```

This logic already exists! ✅

## Risk Manager Updates

`allowed_actions()` now needs to reflect two-sided logic:

```python
def allowed_actions(self, market_state):
    inventory = market_state.inventory
    max_short = self.params.get('MM_MAX_SHORT_POS', self.params.get('MM_MAX_POS', 5))
    max_long = self.params.get('MM_MAX_LONG_POS', self.params.get('MM_MAX_POS', 5))
    
    allow = {
        "allow_quote": True,
        "allow_yes_buy": True,    # BUY YES
        "allow_no_buy": True,     # BUY NO
        "reason": ""
    }
    
    # Too short: only allow NO_BUY (which reduces short)
    if inventory <= -max_short:
        allow["allow_yes_buy"] = False
        allow["reason"] = "MAX_SHORT_FLATTEN_ONLY"
        return allow
    
    # Too long: only allow YES_BUY to flatten? NO - YES_BUY adds long
    # Actually: only allow NO_BUY to reduce long
    if inventory >= max_long:
        allow["allow_yes_buy"] = False
        allow["reason"] = "MAX_LONG_FLATTEN_ONLY"
        return allow
    
    return allow
```

Update engine.py to check these before placing each side.

## Checklist

- [ ] Update all `target.bid_*` → `target.yes_bid_*` in BID block (lines 689-894)
- [ ] Update all `target.ask_*` → `target.no_bid_*` in ASK block (lines 900-1090)
- [ ] Update all `mr.working_bid` → `mr.working_yes_buy`
- [ ] Update all `mr.working_ask` → `mr.working_no_buy`
- [ ] Update internal_side markers: `'BID'` → `'YES_BUY'`, `'ASK'` → `'NO_BUY'`
- [ ] **CRITICAL**: Line ~1003: `side="yes", action="sell"` → `side="no", action="buy"`
- [ ] Add allowed_actions() checks before each order placement
- [ ] Verify fill handling for NO side orders
- [ ] Test end-to-end with two bids per market

## Expected Behavior After Refactor

1. Engine computes YES bid price (e.g., 52¢)
2. Derives NO bid price (100 - 52 = 48¢)
3. Places TWO buy orders:
   - Order A: side="yes", action="buy", price=52¢ (BUY YES)
   - Order B: side="no", action="buy", price=48¢ (BUY NO)
4. When either fills:
   - BUY YES fill: inventory += size (net YES position increases)
   - BUY NO fill: inventory -= size (net YES position decreases)
5. Inventory naturally mean-reverts
6. No more accidental shorts from repeated SELL YES orders
