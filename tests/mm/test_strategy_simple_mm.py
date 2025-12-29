from kalshifolder.mm.strategy.simple_mm import compute_quotes


def test_compute_quotes_mid_and_skew():
    bb = 99.0
    ba = 101.0
    inventory = 1.0
    max_pos = 5.0
    edge_ticks = 1
    tick_size = 0.5
    size = 1.0
    q = compute_quotes(bb, ba, inventory, max_pos, edge_ticks, tick_size, size)
    assert q.bid_px is not None and q.ask_px is not None
    assert q.bid_px < q.ask_px
