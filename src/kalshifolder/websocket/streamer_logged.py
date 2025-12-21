# src/kalshifolder/websocket/streamer.py
import os, sys, asyncio, time
from datetime import datetime, timezone
from typing import Dict, Any
from collections import defaultdict, deque

from dotenv import load_dotenv, find_dotenv

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

# If you ever run this file directly (not with -m), ensure src is on sys.path:
THIS_DIR = os.path.dirname(__file__)
SRC_ROOT = os.path.abspath(os.path.join(THIS_DIR, "..", ".."))
if SRC_ROOT not in sys.path:
    sys.path.append(SRC_ROOT)


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _dbg_env():
    session_logger.info("=== Environment Configuration ===")
    session_logger.info(f"MARKETS: {os.getenv('MARKET_TICKERS')}")
    session_logger.info(f"CH_URL: {os.getenv('CLICKHOUSE_URL')}")
    session_logger.info(f"CH_USER: {os.getenv('CLICKHOUSE_USER')}")
    session_logger.info(f"CH_DATABASE: {os.getenv('CLICKHOUSE_DATABASE')}")
    session_logger.info("==================================\n")


def _extract_ticker_and_market_id(frame: dict) -> tuple[str | None, str | None]:
    """
    Kalshi frames have varied shapes depending on message type.
    We try multiple locations defensively.
    """
    msg = frame.get("msg") or {}
    ticker = (
        msg.get("market_ticker")
        or frame.get("market_ticker")
        or msg.get("ticker")
        or frame.get("ticker")
    )
    market_id = (
        msg.get("market_id")
        or frame.get("market_id")
    )
    if isinstance(ticker, str):
        ticker = ticker.strip() or None
    if isinstance(market_id, str):
        market_id = market_id.strip() or None
    return ticker, market_id


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

        await asyncio.sleep(0.2)

        # IMPORTANT:
        # Your logs prove Kalshi can reuse the same sid across many subscribed tickers.
        # Therefore, subscribing "one ticker at a time" does NOT guarantee unique sids.
        # We subscribe once with the full list; correctness comes from ticker-keyed books.
        session.log_event(f"Subscribing to markets: {tickers}")
        await rt.subscribe_markets(tickers)

        # ------------------------------------------------------------------
        # CRITICAL FIX (the one-sid disaster):
        # We key books by market_ticker (authoritative), NOT sid.
        # ------------------------------------------------------------------
        books: Dict[str, OrderBook] = {}              # ticker -> OrderBook
        ticker_to_market_id: Dict[str, str] = {}      # ticker -> market_id (best effort)

        # If deltas arrive before snapshot, buffer per ticker (only possible if delta includes ticker)
        pending_deltas: Dict[str, deque] = defaultdict(lambda: deque(maxlen=10_000))

        # Stats
        events_processed = 0
        snapshots_received = 0
        deltas_received = 0
        events_total = 0

        # Gap / negative-level monitoring
        gap_warn_counts = defaultdict(int)
        last_resnapshot_monotonic: Dict[str, float] = {}
        RESNAPSHOT_MIN_INTERVAL = 60.0

        # Per-ticker heartbeat stats
        ticker_stats: Dict[str, Dict[str, object]] = {}
        # Per-ticker sequence / gap / resnapshot metrics
        ticker_seq_metrics: Dict[str, Dict[str, int]] = {}

        async def heartbeat_loop():
            while True:
                try:
                    # ClickHouse insert stats
                    istats = getattr(ch, "insert_stats", None)
                    if istats:
                        insert_failures_total = istats.insert_failures_total
                        insert_failures_recent = istats.recent_fail_count()
                        last_err_age = (
                            f"{(datetime.now(timezone.utc).timestamp() - istats.last_insert_error_ts):.0f}s"
                            if istats.last_insert_error_ts
                            else "-"
                        )
                        last_err = istats.last_insert_error or "-"
                        last_success = (
                            datetime.fromtimestamp(istats.last_success_insert_ts, timezone.utc).isoformat()
                            if istats.last_success_insert_ts
                            else "-"
                        )
                        pending_rows = getattr(istats, "pending_rows", 0)
                    else:
                        insert_failures_total = getattr(ch, "insert_failures", 0)
                        insert_failures_recent = 0
                        last_err_age = "-"
                        last_err = "-"
                        last_success = getattr(ch, "last_insert_ts", "-")
                        pending_rows = 0

                    ws_age = rt.last_ws_message_age
                    last_ws_s = f"{ws_age:.1f}s" if ws_age != float("inf") else "-"

                    session_logger.info(
                        f"streamer_heartbeat events_total={events_total} "
                        f"insert_failures_total={insert_failures_total} "
                        f"insert_failures_recent={insert_failures_recent} "
                        f"pending_rows={pending_rows} last_success_insert={last_success} "
                        f"last_insert_error_age={last_err_age} last_insert_error={last_err} "
                        f"last_ws_message_age={last_ws_s}"
                    )

                    now_dt = datetime.now(timezone.utc)
                    if ticker_stats and len(ticker_stats) <= 25:
                        parts = []
                        for tkr, stats in ticker_stats.items():
                            last_ts = stats.get("last_ts")
                            if isinstance(last_ts, datetime):
                                age = (now_dt - last_ts).total_seconds()
                                age_s = f"{age:.0f}s"
                            else:
                                age_s = "-"
                            parts.append(
                                f"{tkr}: events={stats.get('events', 0)}, "
                                f"snap={stats.get('snapshots', 0)}, "
                                f"delta={stats.get('deltas', 0)}, "
                                f"age={age_s}"
                            )
                        # Enrich with sequence/gap/resnap metrics when available
                        enriched = []
                        for p in parts:
                            enriched.append(p)
                        for tkr in list(ticker_stats.keys()):
                            m = ticker_seq_metrics.get(tkr)
                            if not m:
                                continue
                            deltas = m.get('deltas_total', 0)
                            resnaps = m.get('resnap_requests', 0)
                            gap_count = m.get('gap_count', 0)
                            gap_total = m.get('gap_total', 0)
                            avg_gap = (gap_total / gap_count) if gap_count else 0
                            enriched.append(
                                f"{tkr}: deltas={deltas} resnaps={resnaps} avg_gap={avg_gap:.2f} max_gap={m.get('max_gap',0)}"
                            )
                        session_logger.info("ticker_health " + " | ".join(enriched))

                except Exception:
                    logger.exception("Failed to emit heartbeat")
                await asyncio.sleep(60)

        heartbeat_task = asyncio.create_task(heartbeat_loop())

        async def _ingest_snapshot_rows(*, sid: int, seq: int, market_id: str, ticker: str, msg: dict):
            ingest_ts = now_utc()
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

        async def _ingest_delta_row(*, sid: int, seq: int, market_id: str | None, ticker: str | None, msg: dict):
            """
            Always ingest raw deltas into immutable stores.
            If ticker/market_id is missing, we still insert with placeholders so you can QA it.
            """
            ts = datetime.fromisoformat(msg["ts"].replace("Z", "+00:00"))
            ingest_ts = now_utc()

            await ch.add_event(
                {
                    "type": "delta",
                    "sid": sid,
                    "seq": seq,
                    "market_id": market_id or "",
                    "market_ticker": ticker or "",
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
                    "market_id": market_id or "",
                    "market_ticker": ticker or "",
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
                    frame = jloads(raw)
                except Exception as e:
                    session.log_event(f"Failed to parse JSON: {e}", level="warning")
                    continue

                typ = frame.get("type")

                if typ == "subscribed":
                    # Kalshi may emit a single sid for the connection, not per market.
                    sid = int(frame.get("msg", {}).get("sid", -1))
                    session.log_event(f"Subscription confirmed (sid={sid})")
                    continue

                # -----------------------
                # SNAPSHOT
                # -----------------------
                if typ == "orderbook_snapshot":
                    snapshots_received += 1
                    events_processed += 1
                    events_total += 1

                    sid = int(frame["sid"])
                    seq = int(frame["seq"])
                    msg = frame["msg"]

                    ticker, market_id = _extract_ticker_and_market_id(frame)
                    if not ticker or not market_id:
                        session_logger.warning(
                            f"snapshot_missing_identity sid={sid} seq={seq} ticker={ticker} market_id={market_id}"
                        )
                        continue

                    ticker_to_market_id[ticker] = market_id

                    ob = books.get(ticker)
                    if ob is None:
                        ob = OrderBook(market_id, ticker)
                        books[ticker] = ob
                        session.log_event(f"Created orderbook for {ticker} (sid={sid}, market_id={market_id})")

                    ob.apply_snapshot(msg, seq)

                    # Initialize / reset sequence metrics for this ticker
                    m = ticker_seq_metrics.setdefault(
                        ticker,
                        {"last_seq": seq, "deltas_total": 0, "resnap_requests": 0, "gap_total": 0, "gap_count": 0, "max_gap": 0},
                    )
                    m["last_seq"] = seq

                    # Update ticker stats
                    stats = ticker_stats.setdefault(
                        ticker,
                        {"snapshots": 0, "deltas": 0, "events": 0, "last_ts": None, "last_ingest_ts": None},
                    )
                    ingest_ts = now_utc()
                    stats["snapshots"] += 1
                    stats["events"] += 1
                    stats["last_ts"] = ingest_ts
                    stats["last_ingest_ts"] = ingest_ts

                    # Persist snapshot rows
                    await _ingest_snapshot_rows(sid=sid, seq=seq, market_id=market_id, ticker=ticker, msg=msg)

                    # Replay buffered deltas for this ticker (if any)
                    if pending_deltas.get(ticker):
                        session_logger.warning(
                            f"Replaying {len(pending_deltas[ticker])} buffered deltas for {ticker}"
                        )
                        while pending_deltas[ticker]:
                            d = pending_deltas[ticker].popleft()
                            dseq = d["seq"]
                            dmsg = d["msg"]

                            # Update sequence metrics for replayed delta
                            m = ticker_seq_metrics.setdefault(
                                ticker,
                                {"last_seq": None, "deltas_total": 0, "resnap_requests": 0, "gap_total": 0, "gap_count": 0, "max_gap": 0},
                            )
                            try:
                                if m.get("last_seq") is not None:
                                    gap = int(dseq) - int(m.get("last_seq")) - 1
                                    if gap > 0:
                                        m["gap_total"] += gap
                                        m["gap_count"] += 1
                                        if gap > m.get("max_gap", 0):
                                            m["max_gap"] = gap
                            except Exception:
                                pass
                            m["deltas_total"] = m.get("deltas_total", 0) + 1
                            m["last_seq"] = int(dseq)

                            abs_size = ob.apply_delta(dmsg["side"], dmsg["price"], dmsg["delta"], dseq)

                            # Always ingest raw delta
                            await _ingest_delta_row(
                                sid=sid, seq=dseq, market_id=market_id, ticker=ticker, msg=dmsg
                            )

                            # Update latest_levels only if valid
                            if abs_size is not None:
                                dts = datetime.fromisoformat(dmsg["ts"].replace("Z", "+00:00"))
                                await ch.upsert_latest(
                                    {
                                        "market_id": market_id,
                                        "market_ticker": ticker,
                                        "side": dmsg["side"],
                                        "price": int(dmsg["price"]),
                                        "size": int(abs_size),
                                        "ts": dts,
                                        "ingest_ts": now_utc(),
                                    }
                                )

                    continue

                # -----------------------
                # DELTA
                # -----------------------
                if typ == "orderbook_delta":
                    deltas_received += 1
                    events_processed += 1
                    events_total += 1

                    sid = int(frame["sid"])
                    seq = int(frame["seq"])
                    msg = frame["msg"]

                    ticker, market_id = _extract_ticker_and_market_id(frame)

                    # If ticker is missing, we cannot safely apply this delta to any book.
                    if not ticker:
                        session_logger.warning(
                            f"delta_missing_ticker sid={sid} seq={seq} (ingesting raw only; not applying)"
                        )
                        await _ingest_delta_row(sid=sid, seq=seq, market_id=market_id, ticker=None, msg=msg)
                        continue

                    # Prefer known market_id for this ticker if delta doesn't carry it
                    if not market_id:
                        market_id = ticker_to_market_id.get(ticker)

                    ob = books.get(ticker)
                    if ob is None:
                        # We haven't seen a snapshot for this ticker yet; buffer the delta
                        pending_deltas[ticker].append({"seq": seq, "msg": msg})
                        # Track seen deltas for this ticker even before snapshot
                        m = ticker_seq_metrics.setdefault(
                            ticker,
                            {"last_seq": None, "deltas_total": 0, "resnap_requests": 0, "gap_total": 0, "gap_count": 0, "max_gap": 0},
                        )
                        m["deltas_total"] = m.get("deltas_total", 0) + 1
                        try:
                            m_last = m.get("last_seq")
                            if m_last is not None:
                                gap = int(seq) - int(m_last) - 1
                                if gap > 0:
                                    m["gap_total"] += gap
                                    m["gap_count"] += 1
                                    if gap > m.get("max_gap", 0):
                                        m["max_gap"] = gap
                        except Exception:
                            pass
                        m["last_seq"] = int(seq)
                        if len(pending_deltas[ticker]) in (1, 10, 100, 1000):
                            session_logger.warning(
                                f"[x{len(pending_deltas[ticker])}] Delta before snapshot for {ticker}; buffering"
                            )

                        # Still ingest the raw delta
                        await _ingest_delta_row(sid=sid, seq=seq, market_id=market_id, ticker=ticker, msg=msg)

                        # Optionally request snapshot (rate-limited)
                        now_mono = time.monotonic()
                        last_mono = last_resnapshot_monotonic.get(ticker, 0.0)
                        if now_mono - last_mono > RESNAPSHOT_MIN_INTERVAL:
                            try:
                                await rt.subscribe_markets([ticker], _id=9001)
                                last_resnapshot_monotonic[ticker] = now_mono
                                # Record that we requested a resnapshot due to delta-before-snapshot
                                ticker_seq_metrics.setdefault(ticker, {}).setdefault("resnap_requests", 0)
                                ticker_seq_metrics[ticker]["resnap_requests"] = ticker_seq_metrics[ticker].get("resnap_requests", 0) + 1
                                session.log_event(
                                    f"Requested resnapshot for {ticker} after delta-before-snapshot",
                                    level="warning",
                                )
                            except Exception as e:
                                session.log_event(f"Failed to request resnapshot for {ticker}: {e}", level="error")

                        continue

                    # Update ticker stats
                    ts = datetime.fromisoformat(msg["ts"].replace("Z", "+00:00"))
                    stats = ticker_stats.setdefault(
                        ticker,
                        {"snapshots": 0, "deltas": 0, "events": 0, "last_ts": None, "last_ingest_ts": None},
                    )
                    stats["deltas"] += 1
                    stats["events"] += 1
                    stats["last_ts"] = ts
                    stats["last_ingest_ts"] = now_utc()

                    # Sequence / gap / resnap metrics
                    m = ticker_seq_metrics.setdefault(
                        ticker,
                        {"last_seq": None, "deltas_total": 0, "resnap_requests": 0, "gap_total": 0, "gap_count": 0, "max_gap": 0},
                    )
                    try:
                        if m.get("last_seq") is not None:
                            gap = int(seq) - int(m.get("last_seq")) - 1
                            if gap > 0:
                                m["gap_total"] += gap
                                m["gap_count"] += 1
                                if gap > m.get("max_gap", 0):
                                    m["max_gap"] = gap
                    except Exception:
                        pass
                    m["deltas_total"] = m.get("deltas_total", 0) + 1
                    m["last_seq"] = int(seq)
                    # Apply delta to book
                    abs_size = ob.apply_delta(msg["side"], msg["price"], msg["delta"], seq)

                    # Always ingest the raw delta
                    await _ingest_delta_row(sid=sid, seq=seq, market_id=ob.market_id, ticker=ticker, msg=msg)

                    if abs_size is None:
                        key = (ticker, msg["side"], msg["price"])
                        gap_warn_counts[key] += 1
                        count = gap_warn_counts[key]

                        if count in (1, 10, 100, 1000):
                            session.log_event(
                                (
                                    f"[x{count}] Sequence gap or negative level for {ticker} "
                                    f"side={msg['side']} price={msg['price']} delta={msg['delta']} seq={seq}"
                                ),
                                level="warning",
                            )

                        # Record reject in the orderbook; only request resnapshot
                        # when the per-ticker recent rejects reach threshold.
                        should_resnap = False
                        try:
                            should_resnap = ob.record_reject(now_ts=time.time())
                        except Exception:
                            # If anything goes wrong, fall back to conservative behavior
                            should_resnap = True

                        if not should_resnap:
                            # Not enough recent rejects yet; do not request resnapshot
                            continue

                        # Request resnapshot (rate-limited)
                        now_mono = time.monotonic()
                        last_mono = last_resnapshot_monotonic.get(ticker, 0.0)
                        if now_mono - last_mono > RESNAPSHOT_MIN_INTERVAL:
                            try:
                                await rt.subscribe_markets([ticker], _id=9000)
                                last_resnapshot_monotonic[ticker] = now_mono
                                # Record that we requested a resnapshot due to gap/negative-level
                                ticker_seq_metrics.setdefault(ticker, {}).setdefault("resnap_requests", 0)
                                ticker_seq_metrics[ticker]["resnap_requests"] = ticker_seq_metrics[ticker].get("resnap_requests", 0) + 1
                                session.log_event(
                                    f"Requested resnapshot for {ticker} after gap/negative level",
                                    level="warning",
                                )
                            except Exception as e:
                                session.log_event(f"Failed to request resnapshot for {ticker}: {e}", level="error")

                        # Do NOT update latest_levels if book is untrusted
                        continue

                    # Happy path: update latest_levels
                    await ch.upsert_latest(
                        {
                            "market_id": ob.market_id,
                            "market_ticker": ticker,
                            "side": msg["side"],
                            "price": int(msg["price"]),
                            "size": int(abs_size),
                            "ts": ts,
                            "ingest_ts": now_utc(),
                        }
                    )

                    if events_processed % 1000 == 0:
                        session.log_event(
                            f"Processed {events_processed} events ({snapshots_received} snapshots, {deltas_received} deltas)"
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
                f"Final stats: {events_processed} events processed ({snapshots_received} snapshots, {deltas_received} deltas)"
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
