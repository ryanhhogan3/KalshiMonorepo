from backtest.cli import parse_args

def test_run_args_parse_tuning_flags():
    args = parse_args([
        "run",
        "--market", "SAMPLE.MKT",
        "--start", "2025-11-01T15:00:00Z",
        "--end", "2025-11-01T15:01:00Z",
        "--source", "parquet",
        "--path", "x.parquet",
        "--order-size", "3",
        "--max-inv", "6",
        "--min-spread-ticks", "2",
        "--skew-k", "0.5",
    ])
    assert args.order_size == 3
    assert args.max_inv == 6.0
    assert args.min_spread_ticks == 2
    assert args.skew_k == 0.5
