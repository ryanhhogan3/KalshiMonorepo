import os, sys, asyncio
from datetime import datetime, timezone
from dotenv import load_dotenv, find_dotenv
from typing import Dict


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

def _dbg_env():
    print("[ENV] MARKETS   =", os.getenv("MARKET_TICKERS"))
    print("[ENV] CH_URL    =", os.getenv("CLICKHOUSE_URL"))
    print("[ENV] CH_USER   =", os.getenv("CLICKHOUSE_USER"))
    print("[ENV] CH_PWDLEN =", len(os.getenv("CLICKHOUSE_PASSWORD") or ""))
    print("[ENV] USE_CH    =", os.getenv("USE_CLICKHOUSE","1"))

# If you ever run this file directly (not with -m), ensure src is on sys.path:
THIS_DIR = os.path.dirname(__file__)
SRC_ROOT = os.path.abspath(os.path.join(THIS_DIR, "..", ".."))
if SRC_ROOT not in sys.path:
    sys.path.append(SRC_ROOT)

from databases.processing.clickhouse_sink import ClickHouseSink
from databases.processing.parquet_sink import ParquetSink
from kalshi.websocket.ws_runtime import KalshiWSRuntime
from kalshi.websocket.order_book import OrderBook

def now_utc(): return datetime.now(timezone.utc)

async def main():
    _dbg_env()
    tickers = [t.strip() for t in os.getenv("MARKET_TICKERS","").split(",") if t.strip()]
    if not tickers:
        print("Set MARKET_TICKERS in .env", file=sys.stderr); return
    
    have_snapshot = set()  # sids with snapshot

    rt = KalshiWSRuntime()
    ch = ClickHouseSink()  # hot
    pq = ParquetSink(os.getenv("PARQUET_DIR","./data"),
                     rotate_secs=float(os.getenv("PARQUET_ROTATE_SECS","300")),
                     max_rows=int(os.getenv("PARQUET_MAX_ROWS","250000")))  # cold
    print(f"[RUN] markets={tickers}")

    await rt.start()
    print("[WS] waiting for connection…")
    await rt.wait_connected()        # <<< this line relies on A)
    print("[WS] connected, starting consumer…")
    # start consumer before subscribing so we don’t miss the snapshot
    print("[WS] starting consumer…")
    # you can keep your existing loop-or-processing; for visibility add a tiny peek:
    async def _peek():
        m = jloads(await rt.queue.get())
        # don’t consume everything here—just show first few frames then requeue or log
        print(f"[WS] first frame type={m.get('type')}")
        # put it back if you built a separate consumer; or just let the main loop handle it

    asyncio.create_task(_peek())

    await asyncio.sleep(0.2)
    print("[WS] subscribing…")
    await rt.subscribe_markets(tickers)

    # state
    books: Dict[int, OrderBook] = {}      # sid -> OrderBook
    sid_to_ticker: Dict[int, str] = {}    # convenience

    try:
        while True:
            raw = await rt.queue.get()
            try:
                m = jloads(raw)
            except Exception:
                continue

            typ = m.get("type")
            if typ == "subscribed":
                sid = m["msg"]["sid"]
                # we don't get ticker here; the next snapshot will carry it
                continue

            if typ == "orderbook_snapshot":
                sid = int(m["sid"])
                msg = m["msg"]
                ob = books.get(sid)
                if ob is None:
                    ob = OrderBook(msg["market_id"], msg["market_ticker"])
                    books[sid] = ob
                    sid_to_ticker[sid] = msg["market_ticker"]
                ob.apply_snapshot(msg, m["seq"])

                # emit snapshot rows (immutable truth)
                ingest_ts = now_utc()
                def emit_snapshot_side(side):
                    for price, size in msg.get(side, []):
                        row = {
                            "type": "snapshot",
                            "sid": sid, "seq": m["seq"],
                            "market_id": msg["market_id"], "market_ticker": msg["market_ticker"],
                            "side": side, "price": int(price), "qty": int(size),
                            "ts": ingest_ts, "ingest_ts": ingest_ts,
                        }
                        return row
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
                sid = int(m["sid"]); msg = m["msg"]
                ob = books.get(sid)
                if ob is None:
                    # we haven't seen snapshot yet; skip or request resubscribe
                    continue
                abs_size = ob.apply_delta(msg["side"], msg["price"], msg["delta"], m["seq"])
                if abs_size is None:
                    # sequencing gap or negative level -> resync strategy here
                    # (simple approach: ignore and wait for next snapshot)
                    continue

                ts = datetime.fromisoformat(msg["ts"].replace("Z","+00:00"))
                ingest_ts = now_utc()

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
                continue

            # ignore other frame types; you can log if needed

    finally:
        await ch.flush()
        await pq.flush()
        await rt.stop()

if __name__ == "__main__":
    asyncio.run(main())
