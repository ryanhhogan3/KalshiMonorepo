# market_worker.py
import argparse
import asyncio
import json
import os
import sys
import time
from typing import Optional

from ws_runtime import KalshiWSRuntime, build_orderbook_subscribe

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
        self._tasks: list[asyncio.Task] = []
        self._stopping = asyncio.Event()
        self._last_print_ts = 0.0
        self._ndjson_fp = None

    async def start(self):
        if self.ndjson_path:
            os.makedirs(os.path.dirname(self.ndjson_path), exist_ok=True)
            self._ndjson_fp = open(self.ndjson_path, "a", encoding="utf-8")

        await self.rt.start()
        await asyncio.sleep(1.0)  # allow connect
        await self.rt.send(build_orderbook_subscribe(ticker=self.ticker))
        self._tasks.append(asyncio.create_task(self._consumer(), name="consumer"))

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
        """Print-only consumer (no DB)."""
        min_interval = 1.0 / self.rate_limit_hz if self.rate_limit_hz else 0.0

        while not self._stopping.is_set():
            try:
                raw = await self.rt.queue.get()

                # optional capture
                if self._ndjson_fp:
                    self._ndjson_fp.write(raw)
                    if not raw.endswith("\n"):
                        self._ndjson_fp.write("\n")

                # rate limit prints if requested
                now = time.time()
                if min_interval and (now - self._last_print_ts) < min_interval:
                    continue
                self._last_print_ts = now

                if self.raw:
                    sys.stdout.write(raw + ("\n" if not raw.endswith("\n") else ""))
                    sys.stdout.flush()
                    continue

                try:
                    msg = json.loads(raw)
                except Exception:
                    print(raw)
                    continue

                print(_json_dumps(msg, pretty=self.pretty))

            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"[worker] consumer error: {e!r}", file=sys.stderr)

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
