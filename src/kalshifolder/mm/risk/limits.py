from typing import Dict
import logging

logger = logging.getLogger(__name__)


class RiskManager:
    def __init__(self, params: Dict):
        self.params = params
        self.rejects_per_min = {}
        self.kill = False

    def check_market(self, market_state) -> (bool, str):
        # returns (allowed, reason)
        if self.kill:
            return False, 'KILL_SWITCH'
        if market_state.kill_stale and self.params.get('MM_KILL_ON_STALE', 1):
            return False, 'STALE_MARKET'
        
        # CRITICAL FIX: Use abs(inventory) for absolute exposure cap
        inventory = market_state.inventory
        max_pos = self.params.get('MM_MAX_POS', 5)
        
        if abs(inventory) >= max_pos:
            return False, 'MAX_POS'
        
        # Optional: Enforce directional caps if configured
        max_long = self.params.get('MM_MAX_LONG_POS')
        max_short = self.params.get('MM_MAX_SHORT_POS')
        
        if max_long and inventory >= max_long:
            return False, 'MAX_LONG_POS'
        if max_short and inventory <= -max_short:
            return False, 'MAX_SHORT_POS'
        
        return True, ''

    def record_reject(self, engine_id: str, market: str):
        # minimal rolling tracking, increment global; if spike -> kill
        self.rejects_per_min.setdefault(market, 0)
        self.rejects_per_min[market] += 1
        total = sum(self.rejects_per_min.values())
        if total > self.params.get('MM_MAX_REJECTS_PER_MIN', 10) and self.params.get('MM_KILL_ON_REJECT_SPIKE', 1):
            self.kill = True
            logger.warning('Kill triggered by reject spike')

    def is_killed(self) -> bool:
        return self.kill
