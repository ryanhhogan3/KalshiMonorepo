import os, sys, asyncio, time
from datetime import datetime, timezone
from dotenv import load_dotenv, find_dotenv
from typing import Dict, List, Any
from collections import defaultdict, deque

from databases.processing.clickhouse_sink import ClickHouseSink
from databases.processing.parquet_sink import ParquetSink
from kalshifolder.websocket.ws_runtime import KalshiWSRuntime
from kalshifolder.websocket.order_book import OrderBook
from logging_config import get_logger, setup_session_logger
from workflow_logger import AsyncWorkflowSession

# JSON loader (fast path if orjson is available)
try:
    import orjson as _jsonlib
    def jloads(b: str): return _jsonlib.loads(b)
except Exception:
    import json as _jsonlib
    def jloads(b: str): return _jsonlib.loads(b)

# Load .env reliably even when running from src/
env_path = find_dotenv(usecwd=True)
load_dotenv(env_path or None)

# Initialize logging
session_logger = setup_session_logger()
logger = get_logger(__name__)


def _dbg_env():
    session_logger.info("=== Environment Configuration ===")
    session_logger.info(f"MARKETS: {os.getenv('MARKET_TICKERS')}")
    session_logger.info(f"CH_URL: {os.getenv('CLICKHOUSE_URL')}")
    session_logger.info(f"CH_USER: {os.getenv('CLICKHOUSE_USER')}")
    session_logger.info(f"CH_DATABASE: {os.getenv('CLICKHOUSE_DATABASE')}")
    session_logger.info("==================================\n")


# If you ever run this file directly (not with -m), ensure src is on sys.path:
THIS_DIR = os.path.dirname(__file__)
SRC_ROOT = os.path.abspath(os.path.join(THIS_DIR, "..", ".."))
if SRC_ROOT not in sys.path:
    sys.path.append(SRC_ROOT)


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


async def main():
    async with AsyncWorkflowSession("KalshiMonorepo Streamer") as session:
        _dbg_env()

        tickers = [t.strip() for t in os.getenv("MARKET_TICKERS", "").split(",") if t.strip()]
        if not tickers:
            session.log_event("MARKET_TICKERS not set in .env", level="error")
            return

        session.log_event(f"Loaded {len(tickers)} markets: {', '.join(tickers)}")

        rt = KalshiWSRuntime()
        ch = ClickHouseSink()  # hot
        pq = ParquetSink(
            os.getenv("PARQUET_DIR", "./data"),
            max_bytes=100 * 1024 * 1024,
            size_check_every=1000,
            compression_ratio=0.35,
        )
        session.log_event("Initialized WebSocket client and sinks")

        await rt.start(session_logger=session_logger)
        session.log_event("WebSocket runtime started, waiting for connection…")
        await rt.wait_connected()
        session.log_event("Connected to Kalshi WebSocket")

        # ---------------------------------------------------------------------
        # OPTION A (correct): subscribe each ticker individually so Kalshi gives
        # a distinct sid per ticker (instead of collapsing multiple tickers into one sid)
        # ---------------------------------------------------------------------
        await asyncio.sleep(0.2)
        session.log_event(f"Subscribing to markets individually: {tickers}")
        for i, t in enumerate(tickers, start=1):
            await rt.subscribe_markets([t], _id=1000 + i)
            await asyncio.sleep(0.05)

        # --- state ---
        # CRITICAL FIX: key by sid (authoritative). Kalshi routes updates by sid.
        books: Dict[int, OrderBook] = {}               # sid -> OrderBook
        sid_to_ticker: Dict[int, str] = {}             # sid -> ticker (guard rail)
        sid_to_market_id: Dict[int, str] = {}          # sid -> market_id (guard rail)

        # Buffer for "delta before snapshot" (per sid)
        pending_deltas: Dict[int, deque] = defaultdict(lambda: deque(maxlen=5000))

        events_processed = 0
        snapshots_received = 0
        deltas_received = 0

        gap_warn_counts = defaultdict(int)
        last_resnapshot_monotonic: Dict[str, float] = {}
        RESNAPSHOT_MIN_INTERVAL = 60.0  # seconds per ticker between resnapshot requests

        # Per-ticker health stats (for heartbeat logging)
        ticker_stats: Dict[str, Dict[str, object]] = {}

        # heartbeat / monitoring
        events_total = 0

        async def heartbeat_loop():
            while True:
                try:
                    last_insert = getattr(ch, "last_insert_ts", None)
                    last_insert_s = last_insert.isoformat() if last_insert else "-"
                    ws_age = rt.last_ws_message_age
                    last_ws_s = f"{ws_age:.1f}s" if ws_age != float("inf") else "-"
                    msg = (
                        f"streamer_heartbeat events_total={events_total} "
                        f"insert_failures={ch.insert_failures} "
                        f"last_insert_ts={last_insert_s} last_ws_message_age={last_ws_s}"
                    )
                    session_logger.info(msg)

                    now_dt = datetime.now(timezone.utc)

                    if ticker_stats and len(ticker_stats) <= 25:
                        parts = []
                        for ticker, stats in ticker_stats.items():
                            last_ts = stats.get("last_ts")
                            if isinstance(last_ts, datetime):
                                age = (now_dt - last_ts).total_seconds()
                                age_s = f"{age:.0f}s"
                            else:
                                age_s = "-"
                            parts.append(
                                f"{ticker}: events={stats.get('events', 0)}, "
                                f"snap={stats.get('snapshots', 0)}, "
                                f"delta={stats.get('deltas', 0)}, "
                                f"age={age_s}"
                            )
                        session_logger.info("ticker_health " + " | ".join(parts))

                except Exception:
                    logger.exception("Failed to emit heartbeat")
                await asyncio.sleep(60)

        heartbeat_task = asyncio.create_task(heartbeat_loop())

        async def _ingest_delta_row(*, sid: int, seq: int, market_id: str, ticker: str, msg: dict):
            """Insert a raw delta into CH + Parquet (immutable events)."""
            ts = datetime.fromisoformat(msg["ts"].replace("Z", "+00:00"))
            ingest_ts = now_utc()

            await ch.add_event(
                {
                    "type": "delta",
                    "sid": sid,
                    "seq": seq,
                    "market_id": market_id,
                    "market_ticker": ticker,
                    "side": msg["side"],
                    "price": int(msg["price"]),
                    "qty": int(msg["delta"]),
                    "ts": ts,
                    "ingest_ts": ingest_ts,
                }
            )
            await pq.add(
                {
                    "type": "delta",
                    "sid": sid,
                    "seq": seq,
                    "market_id": market_id,
                    "market_ticker": ticker,
                    "side": msg["side"],
                    "price": int(msg["price"]),
                    "qty": int(msg["delta"]),
                    "ts": ts.isoformat(),
                    "ingest_ts": ingest_ts.isoformat(),
                }
            )

        try:
            while True:
                raw = await rt.queue.get()
                try:
                    m = jloads(raw)
                except Exception as e:
                    session.log_event(f"Failed to parse JSON: {e}", level="warning")
                    continue

                typ = m.get("type")

                if typ == "subscribed":
                    sid = int(m["msg"]["sid"])
                    session.log_event(f"Subscription confirmed for sid={sid}")
                    continue

                # ---------------------------
                # SNAPSHOT
                # ---------------------------
                if typ == "orderbook_snapshot":
                    snapshots_received += 1
                    events_processed += 1

                    sid = int(m["sid"])
                    seq = int(m["seq"])
                    msg = m["msg"]
                    ticker = msg["market_ticker"]
                    market_id = msg["market_id"]

                    # Guard rail: sid should never represent multiple tickers
                    if sid in sid_to_ticker and sid_to_ticker[sid] != ticker:
                        session_logger.error(
                            f"SID_COLLISION sid={sid} was={sid_to_ticker[sid]} now={ticker} "
                            f"(this would corrupt data; refusing to overwrite mapping)"
                        )
                        # In this edge case, do not apply snapshot to avoid mixing markets.
                        continue

                    sid_to_ticker[sid] = ticker
                    sid_to_market_id[sid] = market_id

                    ob = books.get(sid)
                    if ob is None:
                        ob = OrderBook(market_id, ticker)
                        books[sid] = ob
                        session.log_event(
                            f"Created orderbook for {ticker} (sid={sid}, market_id={market_id})"
                        )

                    # Apply snapshot to book
                    ob.apply_snapshot(msg, seq)

                    ingest_ts = now_utc()
                    events_total += 1

                    # Update per-ticker stats
                    stats = ticker_stats.setdefault(
                        ticker,
                        {
                            "snapshots": 0,
                            "deltas": 0,
                            "events": 0,
                            "last_ts": None,
                            "last_ingest_ts": None,
                        },
                    )
                    stats["snapshots"] += 1
                    stats["events"] += 1
                    stats["last_ts"] = ingest_ts
                    stats["last_ingest_ts"] = ingest_ts

                    # Emit snapshot rows
                    for side in ("yes", "no"):
                        for price, size in msg.get(side, []):
                            await ch.add_event(
                                {
                                    "type": "snapshot",
                                    "sid": sid,
                                    "seq": seq,
                                    "market_id": market_id,
                                    "market_ticker": ticker,
                                    "side": side,
                                    "price": int(price),
                                    "qty": int(size),
                                    "ts": ingest_ts,
                                    "ingest_ts": ingest_ts,
                                }
                            )
                            await ch.upsert_latest(
                                {
                                    "market_id": market_id,
                                    "market_ticker": ticker,
                                    "side": side,
                                    "price": int(price),
                                    "size": int(size),
                                    "ts": ingest_ts,
                                    "ingest_ts": ingest_ts,
                                }
                            )
                            await pq.add(
                                {
                                    "type": "snapshot",
                                    "sid": sid,
                                    "seq": seq,
                                    "market_id": market_id,
                                    "market_ticker": ticker,
                                    "side": side,
                                    "price": int(price),
                                    "qty": int(size),
                                    "ts": ingest_ts.isoformat(),
                                    "ingest_ts": ingest_ts.isoformat(),
                                }
                            )

                    # If we buffered deltas before this snapshot arrived, replay them now
                    if pending_deltas.get(sid):
                        session_logger.warning(
                            f"Replaying {len(pending_deltas[sid])} buffered deltas for {ticker} (sid={sid})"
                        )
                        while pending_deltas[sid]:
                            dm = pending_deltas[sid].popleft()
                            dmsg = dm["msg"]
                            dseq = int(dm["seq"])

                            # Apply delta to book
                            abs_size = ob.apply_delta(dmsg["side"], dmsg["price"], dmsg["delta"], dseq)

                            # Always ingest raw delta
                            await _ingest_delta_row(
                                sid=sid,
                                seq=dseq,
                                market_id=market_id,
                                ticker=ticker,
                                msg=dmsg,
                            )

                            # Update latest_levels only if book update was valid
                            if abs_size is not None:
                                ts = datetime.fromisoformat(dmsg["ts"].replace("Z", "+00:00"))
                                ingest_ts2 = now_utc()
                                await ch.upsert_latest(
                                    {
                                        "market_id": market_id,
                                        "market_ticker": ticker,
                                        "side": dmsg["side"],
                                        "price": int(dmsg["price"]),
                                        "size": int(abs_size),
                                        "ts": ts,
                                        "ingest_ts": ingest_ts2,
                                    }
                                )

                    continue

                # ---------------------------
                # DELTA
                # ---------------------------
                if typ == "orderbook_delta":
                    deltas_received += 1
                    events_processed += 1

                    sid = int(m["sid"])
                    seq = int(m["seq"])
                    msg = m["msg"]

                    ob = books.get(sid)
                    if ob is None:
                        # We don't yet know ticker/market_id; buffer this delta until snapshot arrives
                        pending_deltas[sid].append({"seq": seq, "msg": msg})
                        if len(pending_deltas[sid]) in (1, 10, 100, 1000):
                            session_logger.warning(
                                f"[x{len(pending_deltas[sid])}] Delta received before snapshot for sid={sid}; buffering"
                            )
                        continue

                    ticker = ob.ticker
                    market_id = ob.market_id

                    # Update per-ticker stats
                    ts = datetime.fromisoformat(msg["ts"].replace("Z", "+00:00"))
                    ingest_ts = now_utc()
                    events_total += 1

                    stats = ticker_stats.setdefault(
                        ticker,
                        {
                            "snapshots": 0,
                            "deltas": 0,
                            "events": 0,
                            "last_ts": None,
                            "last_ingest_ts": None,
                        },
                    )
                    stats["deltas"] += 1
                    stats["events"] += 1
                    stats["last_ts"] = ts
                    stats["last_ingest_ts"] = ingest_ts

                    abs_size = ob.apply_delta(msg["side"], msg["price"], msg["delta"], seq)

                    if abs_size is None:
                        key = (ticker, msg["side"], msg["price"])
                        gap_warn_counts[key] += 1
                        count = gap_warn_counts[key]

                        if count in (1, 10, 100, 1000):
                            session.log_event(
                                (
                                    f"[x{count}] Sequence gap or negative level for {ticker} "
                                    f"side={msg['side']} price={msg['price']} "
                                    f"delta={msg['delta']} seq={seq}"
                                ),
                                level="warning",
                            )

                        # Resnapshot logic (rate-limited)
                        now_mono = time.monotonic()
                        last_mono = last_resnapshot_monotonic.get(ticker, 0.0)
                        if now_mono - last_mono > RESNAPSHOT_MIN_INTERVAL:
                            try:
                                await rt.subscribe_markets([ticker], _id=9000)
                                last_resnapshot_monotonic[ticker] = now_mono
                                session.log_event(
                                    f"Requested resnapshot for {ticker} after gap/negative level",
                                    level="warning",
                                )
                            except Exception as e:
                                session.log_event(
                                    f"Failed to request resnapshot for {ticker}: {e}",
                                    level="error",
                                )

                        # Always store the raw delta
                        await _ingest_delta_row(
                            sid=sid,
                            seq=seq,
                            market_id=market_id,
                            ticker=ticker,
                            msg=msg,
                        )
                        # Do NOT update latest_levels if we don't trust the book
                        continue

                    # Happy path: store delta + update latest_levels
                    await _ingest_delta_row(
                        sid=sid,
                        seq=seq,
                        market_id=market_id,
                        ticker=ticker,
                        msg=msg,
                    )
                    await ch.upsert_latest(
                        {
                            "market_id": market_id,
                            "market_ticker": ticker,
                            "side": msg["side"],
                            "price": int(msg["price"]),
                            "size": int(abs_size),
                            "ts": ts,
                            "ingest_ts": ingest_ts,
                        }
                    )

                    # Log every 1000 events
                    if events_processed % 1000 == 0:
                        session.log_event(
                            f"Processed {events_processed} events "
                            f"({snapshots_received} snapshots, {deltas_received} deltas)"
                        )
                    continue

                session_logger.debug(f"Unknown frame type: {typ}")

        except KeyboardInterrupt:
            session.log_event("Received KeyboardInterrupt, shutting down gracefully...")
        except Exception as e:
            session.log_event(f"Unexpected error in main loop: {e}", level="error")
            raise
        finally:
            try:
                heartbeat_task.cancel()
                await heartbeat_task
            except Exception:
                pass

            session.log_event("Flushing remaining data to sinks...")
            try:
                await ch.flush()
            except Exception:
                logger.exception("Failed to flush ClickHouse sink")
            try:
                await pq.flush()
            except Exception:
                logger.exception("Failed to flush Parquet sink")
            try:
                await rt.stop()
            except Exception:
                logger.exception("Failed to stop WebSocket runtime")

            session.log_event(
                f"Final stats: {events_processed} events processed "
                f"({snapshots_received} snapshots, {deltas_received} deltas)"
            )
            session_logger.info("=== SESSION COMPLETE ===")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)
