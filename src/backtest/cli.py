"""CLI entrypoint for running a quick backtest.

Usage:
    python -m backtest.cli run --market TICKER --start 2025-11-01T10:00:00Z --end 2025-11-01T10:05:00Z --source clickhouse

"""
import argparse
import os
from datetime import datetime
from backtest.replay.event_stream import get_event_stream
from backtest.replay.book_builder import BookState
from backtest.sim.broker import Broker
from backtest.sim.fill_model import check_fills
from backtest.sim.portfolio import Portfolio
from backtest.metrics.tracker import MetricsTracker
from backtest.sim.types import SimConfig
from backtest.strategy.baseline_mm import strategy_step


def parse_args(argv=None):
    parser = argparse.ArgumentParser(prog="backtest")
    sub = parser.add_subparsers(dest="cmd")

    run = sub.add_parser("run", help="Run a backtest")
    run.add_argument("--market", required=True)
    run.add_argument("--start", required=True)
    run.add_argument("--end", required=True)
    run.add_argument("--source", choices=["clickhouse", "parquet"], default="clickhouse")
    run.add_argument("--cache", action="store_true", help="Write a parquet cache for faster iteration")
    run.add_argument("--fixture", help="Path to parquet fixture (for parquet source)")

    gen = sub.add_parser("generate-fixture", help="Generate a small synthetic parquet fixture for smoke tests")
    gen.add_argument("--out", default=os.path.join("tests", "fixtures", "sample_market_1min.parquet"))

    return parser.parse_args(argv)


def cmd_run(args):
    # parse times as ISO
    tstart = datetime.fromisoformat(args.start.replace("Z", "+00:00"))
    tend = datetime.fromisoformat(args.end.replace("Z", "+00:00"))

    # event stream yields snapshot batches and delta events
    stream = get_event_stream(source=args.source, market=args.market, t_start=tstart, t_end=tend, fixture_path=args.fixture, cache=args.cache)

    book = None
    prev_book = None
    stats = {"events": 0, "snapshots": 0, "deltas": 0}

    # sim components
    broker = Broker()
    config = SimConfig()
    portfolio = Portfolio(cash=0.0, inventory=0.0)
    run_dir = os.path.join('runs', f"run_{args.market}_{args.start}_{args.end}")
    tracker = MetricsTracker(run_dir)

    # For the clickhouse path we may want to capture the full window to write cache.
    # get_event_stream currently yields snapshot then deltas. For clickhouse we'll also fetch the full window inside the reader.
    for ev in stream:
        kind = ev.get("kind")
        if kind == "snapshot":
            book = BookState.from_snapshot_batch(ev)
            stats["snapshots"] += 1
        elif kind == "delta":
            if book is None:
                raise RuntimeError("Replay window must begin with a snapshot; no snapshot seen before events")
            # event timestep: previous book -> new book after applying delta
            prev_book = book
            book.apply_delta(ev["row"])
            stats["deltas"] += 1

            # strategy produces actions based on current book and portfolio
            actions = strategy_step(book, portfolio.inventory, config, broker.all_orders())
            for act in actions:
                o = broker.apply_action(act, ts_ms=ev.get('ts_ms'))
                if o:
                    tracker.record_order(o)
                    tracker.incr('quote_count')

            # check fills deterministically based on prev_book -> book and open orders
            fills = check_fills(prev_book, book, broker.all_orders(), slippage_ticks=config.slippage_ticks, tick_size=config.tick_size)
            for f in fills:
                # apply fill to portfolio
                portfolio.apply_fill(f)
                tracker.record_fill(f)
                # remove filled order from broker
                broker.cancel(f.order_id, ts_ms=f.ts_ms)

            # record metrics at this ts
            mid = None
            if book.best_bid is not None and book.best_ask is not None:
                mid = (book.best_bid + book.best_ask) / 2.0
            else:
                # fallback: try prev_book mid
                if prev_book and prev_book.best_bid is not None and prev_book.best_ask is not None:
                    mid = (prev_book.best_bid + prev_book.best_ask) / 2.0

            equity_val = portfolio.equity(mid if mid is not None else 0.0)
            tracker.record_equity(ev.get('ts_ms') or 0, portfolio.inventory, portfolio.cash, equity_val)
        else:
            # unknown kind; skip
            continue
        stats["events"] += 1

    print("Run complete")
    print(stats)
    if book:
        print("Final best bid/ask:", book.best_bid, book.best_ask)
        print("Top levels:", {
            'best_bid': book.best_bid, 'best_bid_size': book.bids.get(book.best_bid),
            'best_ask': book.best_ask, 'best_ask_size': book.asks.get(book.best_ask)
        })

    # handle explicit caching: if source is clickhouse and --cache was passed, write the full window to data/cache
    if args.source == 'clickhouse' and args.cache:
        try:
            from backtest.data.clickhouse_reader import fetch_window, write_cache
            start_ms = int(tstart.timestamp() * 1000)
            end_ms = int(tend.timestamp() * 1000)
            rows = fetch_window(args.market, start_ms, end_ms)
            cache_path = os.path.join('data', 'cache', f"{args.market}_{args.start}_{args.end}.parquet")
            os.makedirs(os.path.dirname(cache_path), exist_ok=True)
            write_cache(rows, cache_path)
            print(f"Wrote cache to {cache_path}")
        except Exception as e:
            print("Failed to write cache:", e)
    # dump metrics
    try:
        tracker.dump()
        print(f"Wrote run outputs to {run_dir}")
    except Exception:
        pass


def cmd_generate_fixture(args):
    # lazy import to avoid extra deps for simple runs
    from tests.fixtures.generate_fixture import write_sample_parquet

    out = args.out
    os.makedirs(os.path.dirname(out), exist_ok=True)
    write_sample_parquet(out)
    print(f"Wrote fixture to {out}")


def main(argv=None):
    args = parse_args(argv)
    if args.cmd == "run":
        cmd_run(args)
    elif args.cmd == "generate-fixture":
        cmd_generate_fixture(args)
    else:
        print("No command specified. Use `run` or `generate-fixture`.")


if __name__ == "__main__":
    main()
