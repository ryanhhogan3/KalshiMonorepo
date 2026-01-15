from typing import Dict, Tuple
import logging

logger = logging.getLogger(__name__)


class RiskManager:
    def __init__(self, params: Dict):
        self.params = params
        self.rejects_per_min = {}
        self.kill = False

    def check_market(self, market_state, exchange_position: float = 0.0, intended_delta: float = 0.0) -> Tuple[bool, str]:
        # returns (allowed, reason)
        # Binary gate: can we quote this market at all?
        if self.kill:
            return False, 'KILL_SWITCH'
        if market_state.kill_stale and self.params.get('MM_KILL_ON_STALE', 1):
            return False, 'STALE_MARKET'
        
        # CRITICAL FIX: Use exchange position (source of truth) not internal inventory
        # Block if current position >= cap OR projection would exceed cap
        max_pos = self.params.get('MM_MAX_POS', 5)
        
        # Hard block: already at or over the cap
        if abs(exchange_position) >= max_pos:
            return False, f'AT_OR_OVER_CAP (pos={exchange_position}, max={max_pos})'
        
        # Projection block: this order would push us over cap
        projected_position = exchange_position + intended_delta
        if abs(projected_position) > max_pos:
            return False, f'WOULD_EXCEED_CAP (pos={exchange_position}, delta={intended_delta}, max={max_pos})'
        
        # If we get here, market is allowed (but may be flatten-only, see allowed_actions)
        return True, ''

    def allowed_actions(self, market_state, exchange_position: float = 0.0):
        """
        Per-side gating to enable "flatten-only" mode when at/near position limits.
        Uses exchange_position (source of truth) instead of internal inventory.
        
        Returns dict with side-level permissions for safer recovery from edge positions.
        
        Structure:
        {
            "allow_quote": bool,         # can we quote both sides?
            "allow_buy_yes": bool,       # can we add YES contracts?
            "allow_buy_no": bool,        # can we add NO contracts?
            "reason": str                # explanation if restricted
        }
        """
        max_pos = self.params.get('MM_MAX_POS', 5)
        max_long = self.params.get('MM_MAX_LONG_POS', max_pos)
        max_short = self.params.get('MM_MAX_SHORT_POS', max_pos)
        
        # Default: all actions allowed
        allow = {
            "allow_quote": True,
            "allow_buy_yes": True,
            "allow_buy_no": True,
            "reason": ""
        }
        
        # If position is negative (short) and at/approaching max short cap
        # Allow quotes but only actions that reduce short (flatten toward 0)
        if exchange_position <= -max_short:
            allow["allow_quote"] = True
            # Buying NO increases short (adds to negative), so block it
            # Selling NO (buying YES) reduces short, so allow
            allow["allow_buy_no"] = False
            allow["reason"] = "MAX_SHORT_FLATTEN_ONLY"
            return allow
        
        # If position is positive (long) and at/approaching max long cap
        # Allow quotes but only actions that reduce long (flatten toward 0)
        if exchange_position >= max_long:
            allow["allow_quote"] = True
            # Buying YES increases long (adds to positive), so block it
            # Selling YES (buying NO) reduces long, so allow
            allow["allow_buy_yes"] = False
            allow["reason"] = "MAX_LONG_FLATTEN_ONLY"
            return allow
        
        return allow

    def record_reject(self, engine_id: str, market: str):
        # minimal rolling tracking, increment global; if spike -> kill
        self.rejects_per_min.setdefault(market, 0)
        self.rejects_per_min[market] += 1
        total = sum(self.rejects_per_min.values())
        if total > self.params.get('MM_MAX_REJECTS_PER_MIN', 10) and self.params.get('MM_KILL_ON_REJECT_SPIKE', 1):
            self.kill = True
            logger.warning('Kill triggered by reject spike')

    def log_order_placement(self, market_ticker: str, action: str, side: str, count: int, price_cents: int, inventory_before: int):
        """
        Log order placement for validation that flatten-only logic is working correctly.
        
        Args:
            market_ticker: e.g., "SAMPLE.MKT"
            action: "buy" or "sell"
            side: "yes" or "no"
            count: contract count
            price_cents: price in cents
            inventory_before: position before this order
        """
        logger.info(
            "order_placed",
            extra={
                "market_ticker": market_ticker,
                "action": action,
                "side": side,
                "count": count,
                "price_cents": price_cents,
                "inventory_before": inventory_before,
                "direction_impact": f"{action}_{side}"
            }
        )

    def is_killed(self) -> bool:
        return self.kill
