import os, sys, asyncio, json, time, sys
import argparse
from kalshifolder.websocket.ws_runtime import KalshiWSRuntime
from typing import Optional
from kalshifolder.websocket.order_book import OrderBook

THIS_DIR = os.path.dirname(__file__)
SRC_ROOT = os.path.abspath(os.path.join(THIS_DIR, "..", ".."))
if SRC_ROOT not in sys.path:
    sys.path.append(SRC_ROOT)

#if you’re using the new subscribe helper, import it too (optional)

DEFAULT_TICKER = "KXBTCMAXY-25-DEC31-129999.99"

def _json_dumps(obj, pretty: bool, ensure_ascii=False):
    if pretty:
        return json.dumps(obj, indent=2, sort_keys=True, ensure_ascii=ensure_ascii)
    return json.dumps(obj, separators=(",", ":"), ensure_ascii=ensure_ascii)

class MarketWorker:
    def __init__(
        self,
        ticker: str,
        pretty: bool = True,
        raw: bool = False,
        rate_limit_hz: Optional[float] = None,
        ndjson_path: Optional[str] = None,
    ):
        self.ticker = ticker
        self.pretty = pretty
        self.raw = raw
        self.rate_limit_hz = rate_limit_hz
        self.ndjson_path = ndjson_path

        self.rt = KalshiWSRuntime()
        self._have_snapshot = set()
        self._sid_for_ticker = {}
        self._tasks: list[asyncio.Task] = []
        self._stopping = asyncio.Event()
        self._last_print_ts = 0.0
        self._ndjson_fp = None

    async def start(self):
        await self.rt.start()
        # 1) start consumer first
        self._tasks.append(asyncio.create_task(self._consumer(), name="consumer"))
        await asyncio.sleep(0.2)  # tiny settle, optional

        # 2) then subscribe
        print(f"[WS] subscribing to {self.ticker}")
        await self.rt.subscribe_markets([self.ticker])

    async def stop(self):
        self._stopping.set()
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        if self._ndjson_fp:
            self._ndjson_fp.flush()
            self._ndjson_fp.close()
        await self.rt.stop()

    async def _consumer(self):
        while not self._stopping.is_set():
            try:
                raw = await self.rt.queue.get()
                m = json.loads(raw)
                typ = m.get("type")

                if typ == "subscribed":
                    sid = m["msg"]["sid"]
                    # start a short timer to verify snapshot arrives
                    asyncio.create_task(self._ensure_snapshot(sid))
                    print(f"[SUB] sid={sid}")
                    continue

                if typ == "orderbook_snapshot":
                    sid = m["sid"]
                    self._have_snapshot.add(sid)
                    # (optional) remember which ticker if present on snapshot
                    if "market_ticker" in m.get("msg", {}):
                        self._sid_for_ticker[sid] = m["msg"]["market_ticker"]
                    print(f"[SNAP] sid={sid} seq={m['seq']}")
                    # (you can also pretty-print the snapshot here if you want)
                    continue

                if typ == "orderbook_delta":
                    sid = m["sid"]
                    if sid not in self._have_snapshot:
                        # got a delta before seeing the snapshot → force a resubscribe to get a fresh snapshot
                        print(f"[WARN] delta before snapshot (sid={sid}, seq={m.get('seq')}). Resubscribing…")
                        await self.rt.subscribe_markets([self.ticker])
                        continue
                    # normal delta handling/printing
                    print(json.dumps(m, separators=(",", ":")))
                    continue

                if typ == "error":
                    print(f"[ERR] {json.dumps(m, separators=(',',':'))}")
                    continue

                # default: print unknown frames for visibility
                print(json.dumps(m, separators=(",", ":")))

            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"[worker] consumer error: {e!r}")

    async def _ensure_snapshot(self, sid: int, timeout: float = 1.0):
        await asyncio.sleep(timeout)
        if sid not in self._have_snapshot:
            print(f"[WARN] no snapshot within {timeout:.1f}s for sid={sid}. Resubscribing…")
            await self.rt.subscribe_markets([self.ticker])


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--ticker", default=os.getenv("MARKET_TICKER", DEFAULT_TICKER))
    parser.add_argument("--raw", action="store_true", help="Print raw frames (no JSON parsing).")
    parser.add_argument("--compact", action="store_true", help="Compact JSON (no pretty indent).")
    parser.add_argument("--rate", type=float, default=None,
                        help="Max prints per second (e.g., 5). Omit for no limit.")
    parser.add_argument("--ndjson", default=os.getenv("NDJSON_PATH", None),
                        help="Optional path to save frames as NDJSON (no DB).")
    args = parser.parse_args()

    worker = MarketWorker(
        ticker=args.ticker,
        pretty=not args.compact,
        raw=args.raw,
        rate_limit_hz=args.rate,
        ndjson_path=args.ndjson,
    )

    # Start → wait forever; on Ctrl+C, asyncio.run() cancels this task.
    await worker.start()
    try:
        await asyncio.Event().wait()
    finally:
        # Always run on cancellation/KeyboardInterrupt
        await worker.stop()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        # Extra safety (usually not reached because finally handles cleanup)
        pass
