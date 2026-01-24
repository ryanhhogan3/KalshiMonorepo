from kalshifolder.mm.risk.limits import RiskManager
from kalshifolder.mm.state.models import MarketRuntimeState


def test_risk_max_pos_blocks():
    rm = RiskManager({'MM_MAX_POS': 2, 'MM_KILL_ON_STALE': 1, 'MM_KILL_ON_REJECT_SPIKE': 1})
    m = MarketRuntimeState(ticker='X')

    # Over cap: adding more risk should be blocked.
    d_add = rm.check_market(m, exchange_position=32.0, intended_delta=+1.0)
    assert d_add.allowed is False
    assert 'would_exceed_cap' in (d_add.block_codes or [])

    # Over cap: reduce-only actions should still be allowed so the engine can
    # self-heal back under limits.
    d_reduce = rm.check_market(m, exchange_position=32.0, intended_delta=-1.0)
    assert d_reduce.allowed is True
    assert d_reduce.flatten_only is True
    assert 'would_exceed_cap' in (d_reduce.block_codes or [])
