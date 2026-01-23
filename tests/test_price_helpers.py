from kalshifolder.mm.engine import compute_no_bid_px


def test_compute_no_bid_px_basic():
    assert compute_no_bid_px(0.60) == 0.4


def test_compute_no_bid_px_clamps():
    # Values outside (0,1) should clamp to bounds after rounding
    assert compute_no_bid_px(0.0) is None
    assert compute_no_bid_px(1.0) is None
    assert compute_no_bid_px(0.989) == 0.01


def test_compute_no_bid_px_rounding():
    # Ensure rounding to two decimals is deterministic
    assert compute_no_bid_px(0.3333) == 0.67
    assert compute_no_bid_px("0.44") == 0.56
