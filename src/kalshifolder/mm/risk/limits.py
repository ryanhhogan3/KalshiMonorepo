from dataclasses import dataclass, field
from typing import Dict, List
import logging

logger = logging.getLogger(__name__)


@dataclass
class RiskDecision:
    allowed: bool
    block_stage: str = ''
    block_codes: List[str] = field(default_factory=list)
    flatten_only: bool = False
    reason: str = ''


class RiskManager:
    def __init__(self, params: Dict):
        self.params = params
        self.rejects_per_min = {}
        self.kill = False

    def _inventory_bounds(self, market_state) -> Dict[str, float]:
        max_long = float(self.params.get('MM_MAX_LONG_POS', self.params.get('MM_MAX_POS', 5)))
        max_short = float(self.params.get('MM_MAX_SHORT_POS', self.params.get('MM_MAX_POS', 5)))
        exit_ratio = float(self.params.get('MM_INVENTORY_CAP_EXIT_RATIO', 0.9))
        exit_ratio = max(0.0, min(exit_ratio, 1.0))
        return {
            'enter_long': max_long,
            'exit_long': max_long * exit_ratio,
            'enter_short': -max_short,
            'exit_short': -max_short * exit_ratio,
        }

    def _update_hysteresis_flags(self, market_state, pos: float):
        bounds = self._inventory_bounds(market_state)
        if pos >= bounds['enter_long']:
            market_state.flatten_long_active = True
        elif pos <= bounds['exit_long']:
            market_state.flatten_long_active = False

        if pos <= bounds['enter_short']:
            market_state.flatten_short_active = True
        elif pos >= bounds['exit_short']:
            market_state.flatten_short_active = False

    def check_market(self, market_state, exchange_position: float = 0.0, intended_delta: float = 0.0) -> RiskDecision:
        if self.kill:
            return RiskDecision(False, block_stage='risk', block_codes=['kill_switch'], reason='KILL_SWITCH')
        if market_state.kill_stale and self.params.get('MM_KILL_ON_STALE', 1):
            return RiskDecision(False, block_stage='risk', block_codes=['stale_market'], reason='STALE_MARKET')

        pos = float(exchange_position or 0.0)
        self._update_hysteresis_flags(market_state, pos)

        # Pure health check (global call) just reports whether general gating is ok.
        if not intended_delta:
            return RiskDecision(True, reason='')

        projected = pos + intended_delta
        bounds = self._inventory_bounds(market_state)
        max_pos = float(self.params.get('MM_MAX_POS', bounds['enter_long']))

        block_codes: List[str] = []
        flatten_only = False

        if market_state.flatten_long_active:
            flatten_only = True
            if intended_delta > 0:
                block_codes.append('inventory_limit_long')
        if market_state.flatten_short_active:
            flatten_only = True
            if intended_delta < 0:
                block_codes.append('inventory_limit_short')

        if block_codes:
            reduce_only_ok = abs(projected) < abs(pos)
            if reduce_only_ok:
                return RiskDecision(True, block_codes=block_codes, flatten_only=True, reason='REDUCE_ONLY_OK')
            return RiskDecision(False, block_stage='risk', block_codes=block_codes, flatten_only=True, reason='REDUCE_ONLY_BLOCK')

        if abs(projected) > max_pos:
            block_codes.append('would_exceed_cap')
            return RiskDecision(False, block_stage='risk', block_codes=block_codes, reason=f'WOULD_EXCEED_CAP pos={pos} delta={intended_delta}')

        return RiskDecision(True, reason='')

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
