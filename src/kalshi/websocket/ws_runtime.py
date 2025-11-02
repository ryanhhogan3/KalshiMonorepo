import asyncio, base64, json, os, ssl, time
from typing import Optional, Callable, Awaitable, Dict

import certifi, websockets
from websockets.exceptions import ConnectionClosed
from dotenv import load_dotenv
from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.primitives.asymmetric import padding

load_dotenv()

DEFAULT_WS_URL = os.getenv("WS_URL", "wss://api.elections.kalshi.com/trade-api/ws/v2")
DEFAULT_HOST   = os.getenv("WS_HOSTNAME", "api.elections.kalshi.com")
METHOD, PATH   = "GET", "/trade-api/ws/v2"

KEY_ID  = (os.getenv("PROD_KEYID") or "").strip()
KEYFILE = (os.getenv("PROD_KEYFILE") or "").strip()

class KalshiWSRuntime:
    """
    Owns a single WS connection. You call `subscribe_markets(...)`,
    then read raw frames from `queue`. Exposes sid->ticker mapping.
    """
    def __init__(self, *, queue_maxsize: int = 50_000, ping_interval=20.0, ping_timeout=20.0):
        if not KEY_ID or not KEYFILE:
            raise RuntimeError("Missing PROD_KEYID or PROD_KEYFILE")
        with open(KEYFILE, "rb") as f:
            self._private_key = serialization.load_pem_private_key(f.read(), password=None)

        self.ws_url = DEFAULT_WS_URL
        self.server_hostname = DEFAULT_HOST
        self.ping_interval = ping_interval
        self.ping_timeout = ping_timeout

        self._connected = asyncio.Event()

        self._ssl_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        self._ssl_ctx.load_verify_locations(certifi.where())

        self.queue: asyncio.Queue[str] = asyncio.Queue(maxsize=queue_maxsize)
        self._ws = None
        self._run_task = None
        self._stop = asyncio.Event()
        self.sid_to_ticker: Dict[int, str] = {}
        self._backoff_max = 30.0

    def _signed_headers(self):
        ts = str(int(time.time() * 1000))
        msg = (ts + METHOD + PATH).encode("utf-8")
        sig = base64.b64encode(
            self._private_key.sign(
                msg,
                padding.PSS(mgf=padding.MGF1(hashes.SHA256()),
                            salt_length=padding.PSS.DIGEST_LENGTH),
                hashes.SHA256(),
            )
        ).decode("utf-8")
        return [
            ("KALSHI-ACCESS-KEY", KEY_ID),
            ("KALSHI-ACCESS-TIMESTAMP", ts),
            ("KALSHI-ACCESS-SIGNATURE", sig),
        ]

    async def start(self):
        if self._run_task: return
        self._stop.clear()
        self._run_task = asyncio.create_task(self._run_loop(), name="kalshi_ws_run")

    async def stop(self):
        self._stop.set()
        if self._ws:
            try:
                await self._ws.close(code=1000)
                if hasattr(self._ws, "wait_closed"):
                    await self._ws.wait_closed()
            except Exception:
                pass
        if self._run_task:
            self._run_task.cancel()
            try:
                await self._run_task
            except asyncio.CancelledError:
                pass
            self._run_task = None

    async def send(self, payload: str | dict):
        if isinstance(payload, dict):
            payload = json.dumps(payload)
        ws = self._ws
        if ws is None:
            raise RuntimeError("WebSocket not connected")
        try:
            await ws.send(payload)
        except ConnectionClosed:
            raise RuntimeError("WebSocket closed")

    async def subscribe_markets(self, tickers: list[str], *, _id: int = 42):
        payload = {
            "id": _id,
            "cmd": "subscribe",
            "params": {
                "channels": ["orderbook_delta"],
                "market_tickers": tickers,
                # hint to send an initial snapshot; ignored by server if unsupported
                "snapshot": True
            },
        }
        await self.send(payload)

    async def _run_loop(self):
        backoff = 0.5
        while not self._stop.is_set():
            try:
                if self.verbose: print("[WS] connecting...")
                async with websockets.connect(
                    self.ws_url,
                    additional_headers=self._sign_headers(),
                    ssl=self.ssl_ctx,
                    server_hostname=self.server_hostname,
                    open_timeout=25,
                    ping_interval=20,
                    ping_timeout=20,
                ) as ws:
                    self._ws = ws
                    self._connected.set()          # <<< mark ready
                    if self.verbose: print("[WS] connected")
                    backoff = 0.5

                    async for raw in ws:
                        await self.queue.put(raw)

            except Exception as e:
                if self.verbose: print(f"[WS] error: {e!r}")
            finally:
                self._ws = None
                self._connected.clear()            # <<< mark not-ready
                if not self._stop.is_set():
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, 5.0)

    async def wait_connected(self, timeout: float = 5.0):
        await asyncio.wait_for(self._connected.wait(), timeout)

    @property
    def is_connected(self) -> bool:
        return self._connected.is_set()
