import os, sys, asyncio
from datetime import datetime, timezone
from dotenv import load_dotenv, find_dotenv
from typing import Dict
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


def now_utc(): return datetime.now(timezone.utc)

async def main():
    async with AsyncWorkflowSession("KalshiMonorepo Streamer") as session:
        _dbg_env()
        tickers = [t.strip() for t in os.getenv("MARKET_TICKERS","").split(",") if t.strip()]
        if not tickers:
            session.log_event("MARKET_TICKERS not set in .env", level="error")
            return
        
        session.log_event(f"Loaded {len(tickers)} markets: {', '.join(tickers)}")
        have_snapshot = set()  # sids with snapshot

        rt = KalshiWSRuntime()
        ch = ClickHouseSink()  # hot
        pq = ParquetSink(os.getenv("PARQUET_DIR", "./data"),
            max_bytes=100*1024*1024,
            size_check_every=1000,
            compression_ratio=0.35
        )
        session.log_event("Initialized WebSocket client and sinks")

        await rt.start()
        session.log_event("WebSocket runtime started, waiting for connection…")
        await rt.wait_connected()
        session.log_event("Connected to Kalshi WebSocket")
        
        await asyncio.sleep(0.2)
        session.log_event(f"Subscribing to markets: {tickers}")
        await rt.subscribe_markets(tickers)

        # state
        books: Dict[int, OrderBook] = {}      # sid -> OrderBook
        sid_to_ticker: Dict[int, str] = {}    # convenience
        events_processed = 0
        snapshots_received = 0
        deltas_received = 0

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
                    sid = m["msg"]["sid"]
                    session.log_event(f"Subscription confirmed for sid={sid}")
                    continue

                if typ == "orderbook_snapshot":
                    snapshots_received += 1
                    events_processed += 1
                    sid = int(m["sid"])
                    msg = m["msg"]
                    ob = books.get(sid)
                    if ob is None:
                        ob = OrderBook(msg["market_id"], msg["market_ticker"])
                        books[sid] = ob
                        sid_to_ticker[sid] = msg["market_ticker"]
                        session.log_event(f"Created orderbook for {msg['market_ticker']} (sid={sid})")
                    
                    ob.apply_snapshot(msg, m["seq"])
                    session_logger.debug(f"Snapshot received for {msg['market_ticker']}: seq={m['seq']}, sides={'yes' if msg.get('yes') else '?'}, {'no' if msg.get('no') else '?'}")

                    # emit snapshot rows (immutable truth)
                    ingest_ts = now_utc()
                    # ClickHouse (batch)
                    for side in ("yes","no"):
                        for price, size in msg.get(side, []):
                            await ch.add_event({
                                "type":"snapshot","sid":sid,"seq":m["seq"],
                                "market_id":msg["market_id"],"market_ticker":msg["market_ticker"],
                                "side":side,"price":int(price),"qty":int(size),
                                "ts": ingest_ts,"ingest_ts":ingest_ts
                            })
                            # latest absolute
                            await ch.upsert_latest({
                                "market_id":msg["market_id"],"market_ticker":msg["market_ticker"],
                                "side":side,"price":int(price),"size":int(size),
                                "ts": ingest_ts,"ingest_ts":ingest_ts
                            })
                            # parquet archive (events schema)
                            await pq.add({
                                "type":"snapshot","sid":sid,"seq":m["seq"],
                                "market_id":msg["market_id"],"market_ticker":msg["market_ticker"],
                                "side":side,"price":int(price),"qty":int(size),
                                "ts": ingest_ts.isoformat(),"ingest_ts":ingest_ts.isoformat()
                            })
                    continue

                if typ == "orderbook_delta":
                    deltas_received += 1
                    events_processed += 1
                    sid = int(m["sid"])
                    msg = m["msg"]
                    ob = books.get(sid)
                    if ob is None:
                        session.log_event(f"Delta received before snapshot for sid={sid}", level="warning")
                        continue
                    
                    abs_size = ob.apply_delta(msg["side"], msg["price"], msg["delta"], m["seq"])
                    if abs_size is None:
                        session.log_event(f"Sequence gap or negative level for {ob.ticker} at price {msg['price']}", level="warning")
                        continue

                    ts = datetime.fromisoformat(msg["ts"].replace("Z","+00:00"))
                    ingest_ts = now_utc()
                    
                    session_logger.debug(f"Delta: {ob.ticker} {msg['side']} {msg['price']} delta={msg['delta']} => {abs_size}")

                    # immutable event
                    await ch.add_event({
                        "type":"delta","sid":sid,"seq":m["seq"],
                        "market_id":ob.market_id,"market_ticker":ob.ticker,
                        "side":msg["side"],"price":int(msg["price"]),"qty":int(msg["delta"]),
                        "ts": ts,"ingest_ts":ingest_ts
                    })
                    # update latest absolute
                    await ch.upsert_latest({
                        "market_id":ob.market_id,"market_ticker":ob.ticker,
                        "side":msg["side"],"price":int(msg["price"]),"size":int(abs_size),
                        "ts": ts,"ingest_ts":ingest_ts
                    })
                    # archive event
                    await pq.add({
                        "type":"delta","sid":sid,"seq":m["seq"],
                        "market_id":ob.market_id,"market_ticker":ob.ticker,
                        "side":msg["side"],"price":int(msg["price"]),"qty":int(msg["delta"]),
                        "ts": ts.isoformat(),"ingest_ts":ingest_ts.isoformat()
                    })
                    
                    # Log every 1000 events
                    if events_processed % 1000 == 0:
                        session.log_event(f"Processed {events_processed} events ({snapshots_received} snapshots, {deltas_received} deltas)")
                    continue

                # ignore other frame types; log unknown frames
                session_logger.debug(f"Unknown frame type: {typ}")

        except KeyboardInterrupt:
            session.log_event("Received KeyboardInterrupt, shutting down gracefully...")
        except Exception as e:
            session.log_event(f"Unexpected error in main loop: {e}", level="error")
            raise
        finally:
            session.log_event("Flushing remaining data to sinks...")
            await ch.flush()
            await pq.flush()
            await rt.stop()
            session.log_event(f"Final stats: {events_processed} events processed ({snapshots_received} snapshots, {deltas_received} deltas)")
            session_logger.info("=== SESSION COMPLETE ===")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)
