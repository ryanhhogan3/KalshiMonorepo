from kalshifolder.mm.risk.limits import RiskManager
from kalshifolder.mm.state.models import MarketRuntimeState


def test_risk_max_pos_blocks():
    rm = RiskManager({'MM_MAX_POS': 2, 'MM_KILL_ON_STALE': 1, 'MM_KILL_ON_REJECT_SPIKE': 1})
    m = MarketRuntimeState(ticker='X')
    m.inventory = 2
    allowed, reason = rm.check_market(m)
    assert not allowed
    assert reason == 'MAX_POS'
