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
import re
from pathlib import Path


# repository root for deterministic output paths
repo_root = Path(__file__).resolve().parents[2]

def safe_slug(s: str) -> str:
    # Windows-safe: remove or replace characters not allowed in filenames
    # < > : " / \ | ? *
    return re.sub(r'[<>:"/\\|?*]', '-', s)

def run_dir_name(market: str, start: str, end: str) -> str:
    return f"run_{safe_slug(market)}_{safe_slug(start)}_{safe_slug(end)}"


def parse_args(argv=None):
    parser = argparse.ArgumentParser(prog="backtest")
    sub = parser.add_subparsers(dest="cmd")

    run = sub.add_parser("run", help="Run a backtest")
    run.add_argument(
        "--path", "--fixture",
        dest="path",
        help="Path to parquet file (fixture) when --source parquet",
        default=None,
    )
    run.add_argument("--market", required=True)
    run.add_argument("--start", required=True)
    run.add_argument("--end", required=True)
    run.add_argument("--source", choices=["clickhouse", "parquet"], default="clickhouse")
    run.add_argument("--cache", action="store_true", help="Write a parquet cache for faster iteration")
    run.add_argument("--out-dir", default="runs", help="Base output directory for run artifacts")
    run.add_argument("--tick", type=float, default=None, help="Price tick size")
    run.add_argument("--order-size", type=int, default=None, help="Order size per quote")
    run.add_argument("--max-inv", type=float, default=None, help="Max absolute inventory")
    run.add_argument("--min-spread-ticks", type=int, default=None, help="Min spread (in ticks) required to quote both sides")
    run.add_argument("--skew-k", type=float, default=None, help="Inventory skew coefficient (ticks per unit inventory, or your chosen meaning)")
    
    gen = sub.add_parser("generate-fixture", help="Generate a small synthetic parquet fixture for smoke tests")
    gen.add_argument("--out", default=os.path.join("tests", "fixtures", "sample_market_1min.parquet"))

    return parser.parse_args(argv)


def cmd_run(args):
    # parse times as ISO
    tstart = datetime.fromisoformat(args.start.replace("Z", "+00:00"))
    tend = datetime.fromisoformat(args.end.replace("Z", "+00:00"))

    # event stream yields snapshot batches and delta events
    stream = get_event_stream(
    source=args.source,
    market=args.market,
    t_start=tstart,
    t_end=tend,
    fixture_path=args.path,   # <-- THIS is the missing piece
    cache=args.cache,
)
    if args.source == "parquet" and not args.path:
        raise SystemExit("For --source parquet you must provide --path (or --fixture) to a parquet file.")


    book = None
    prev_book = None
    stats = {"events": 0, "snapshots": 0, "deltas": 0}

    # sim components
    broker = Broker()
    config = SimConfig()

    # override defaults if CLI args were provided
    if args.tick is not None:
        config.tick = args.tick
    if args.order_size is not None:
        config.order_size = args.order_size
    if args.max_inv is not None:
        config.max_inv = args.max_inv
    if args.min_spread_ticks is not None:
        config.min_spread_ticks = args.min_spread_ticks
    if args.skew_k is not None:
        config.skew_k = args.skew_k

    portfolio = Portfolio(cash=0.0, inventory=0.0)
    # anchor run directory at repository root so test runs and external invocations
    
    out_dir = Path(args.out_dir)
    base_out = out_dir if out_dir.is_absolute() else (repo_root / out_dir)
    base_out.mkdir(parents=True, exist_ok=True)
    run_dir = base_out / f"run_{safe_slug(args.market)}_{safe_slug(args.start)}_{safe_slug(args.end)}"
    tracker = MetricsTracker(str(run_dir))

    # persistent strategy state for working order ids
    strategy_state = {'bid_id': None, 'ask_id': None}

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
            actions = strategy_step(book, portfolio.inventory, config, broker.all_orders(), strategy_state)
            for act in actions:
                o = broker.apply_action(act, ts_ms=ev.get('ts_ms'))
                if act.kind == 'place' and o:
                    tracker.record_order(o)
                    tracker.incr('quote_count')
                    # store placed order id in strategy state
                    if act.side == 'buy':
                        strategy_state['bid_id'] = o.order_id
                    else:
                        strategy_state['ask_id'] = o.order_id
                elif act.kind == 'modify':
                    tracker.incr('modify_count')
                    # modify keeps the same order id in strategy_state
                elif act.kind == 'cancel':
                    tracker.incr('cancel_count')
                    # clear state if we canceled a tracked id
                    if act.side == 'buy' and strategy_state.get('bid_id') == act.order_id:
                        strategy_state['bid_id'] = None
                    if act.side == 'sell' and strategy_state.get('ask_id') == act.order_id:
                        strategy_state['ask_id'] = None

            # check fills deterministically based on prev_book -> book and open orders
            fills = check_fills(prev_book, book, broker.all_orders(), slippage_ticks=config.slippage_ticks, tick_size=config.tick_size)
            for f in fills:
                # apply fill to portfolio
                portfolio.apply_fill(f)
                tracker.record_fill(f)
                # remove filled order from broker
                broker.cancel(f.order_id, ts_ms=f.ts_ms)
                # if a working order was filled, clear it from strategy_state
                if strategy_state.get('bid_id') == f.order_id:
                    strategy_state['bid_id'] = None
                if strategy_state.get('ask_id') == f.order_id:
                    strategy_state['ask_id'] = None

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
            cache_path = os.path.join(str(repo_root), 'data', 'cache', f"{args.market}_{args.start}_{args.end}.parquet")
            os.makedirs(os.path.dirname(cache_path), exist_ok=True)
            write_cache(rows, cache_path)
            print(f"Wrote cache to {cache_path}")
        except Exception as e:
            print("Failed to write cache:", e)
    # dump metrics (include run-level stats for summary)
    try:
        tracker.dump(stats=stats)
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
