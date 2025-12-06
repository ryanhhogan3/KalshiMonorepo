# src/kalshi/websocket/ws_runtime.py
import asyncio, base64, json, os, ssl, time, random, logging
from typing import Dict, Optional

log = logging.getLogger(__name__)

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
    Owns a single WS connection. Call `subscribe_markets([...])`,
    then read raw frames from `queue` (strings). Use `wait_connected()`
    to wait until the socket is open.
    """
    def __init__(self, *, queue_maxsize: int = 50_000, ping_interval=20.0, ping_timeout=20.0):
        if not KEY_ID or not KEYFILE:
            raise RuntimeError("Missing PROD_KEYID or PROD_KEYFILE")
        with open(KEYFILE, "rb") as f:
            self._private_key = serialization.load_pem_private_key(f.read(), password=None)

        self.ws_url = DEFAULT_WS_URL
        self.server_hostname = DEFAULT_HOST
        self.ping_interval = float(ping_interval)
        self.ping_timeout  = float(ping_timeout)

        # TLS
        self.ssl_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        self.ssl_ctx.load_verify_locations(certifi.where())

        # Runtime state
        self.queue: asyncio.Queue[str] = asyncio.Queue(maxsize=queue_maxsize)
        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        self._run_task: Optional[asyncio.Task] = None
        self._stop = asyncio.Event()
        self._connected = asyncio.Event()  # readiness flag
        self.sid_to_ticker: Dict[int, str] = {}
        self._backoff_max = 30.0
        self.verbose = bool(int(os.getenv("WS_VERBOSE", "1")))

    # ---- signing ----
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

    # ---- lifecycle ----
    async def start(self, session_logger=None) -> None:
        """Idempotently start the WS loop (supervised). Optionally pass a session_logger for session-scoped logs."""
        if self._run_task and not self._run_task.done():
            return
        self._stop.clear()
        self._run_task = asyncio.create_task(self.run_forever(session_logger=session_logger), name="kalshi_ws_run")

    async def stop(self) -> None:
        """Stop loop and close the socket cleanly."""
        self._stop.set()
        if self._ws:
            try:
                await self._ws.close(code=1000)
                if hasattr(self._ws, "wait_closed"):
                    await self._ws.wait_closed()
            except Exception:
                pass
            finally:
                self._ws = None
        if self._run_task:
            self._run_task.cancel()
            try:
                await self._run_task
            except asyncio.CancelledError:
                pass
            self._run_task = None
        self._connected.clear()

    # ---- send/subscribe ----
    async def send(self, payload: str | dict) -> None:
        if isinstance(payload, dict):
            payload = json.dumps(payload)
        if not self._ws:
            raise RuntimeError("WebSocket not connected")
        try:
            await self._ws.send(payload)
        except ConnectionClosed:
            raise RuntimeError("WebSocket closed")

    async def subscribe_markets(self, tickers: list[str], *, _id: int = 42) -> None:
        payload = {
            "id": _id,
            "cmd": "subscribe",
            "params": {
                "channels": ["orderbook_delta"],
                "market_tickers": tickers,
                # hint to send an initial snapshot; ignored if unsupported
                "snapshot": True,
            },
        }
        await self.send(payload)

    # ---- loop helpers ----
    async def _run_once(self):
        """Establish one WS connection and consume until it closes or errors."""
        log.info("ws_connecting", extra={"url": self.ws_url})
        try:
            async with websockets.connect(
                self.ws_url,
                additional_headers=self._signed_headers(),
                ssl=self.ssl_ctx,
                server_hostname=self.server_hostname,
                open_timeout=25,
                ping_interval=self.ping_interval,
                ping_timeout=self.ping_timeout,
            ) as ws:
                self._ws = ws
                self._connected.set()
                log.info("ws_connected")
                async for raw in ws:
                    await self.queue.put(raw)
        finally:
            # ensure state is cleared when connection ends or errors
            try:
                self._ws = None
            except Exception:
                pass
            try:
                self._connected.clear()
            except Exception:
                pass

    async def run_forever(self, session_logger=None):
        """Supervising reconnect loop. Accepts an optional session_logger for session-scoped logs."""
        backoff = 1.0
        while not self._stop.is_set():
            try:
                await self._run_once()
                # if _run_once returns cleanly (socket closed without exception)
                if session_logger:
                    session_logger.warning("ws_run_once_exited_cleanly")
                else:
                    log.warning("ws_run_once_exited_cleanly")
                # brief pause then reset backoff
                await asyncio.sleep(5)
                backoff = 1.0
            except Exception as e:
                # compute jittered sleep and log via session_logger when available
                sleep_for = backoff + random.uniform(0, backoff * 0.1)
                if session_logger:
                    try:
                        session_logger.exception(
                            "ws_error",
                            extra={"context": "ws_consumer", "error": str(e)[:500], "reconnecting_in": sleep_for},
                        )
                    except Exception:
                        log.exception("ws_error (session_logger failed): %s", str(e))
                else:
                    try:
                        log.exception(
                            "ws_error",
                            extra={"context": "ws_consumer", "error": str(e)[:500], "reconnecting_in": sleep_for},
                        )
                    except Exception:
                        log.error("ws_error: %s; reconnecting_in=%s", str(e), sleep_for)

                if self._stop.is_set():
                    break

                await asyncio.sleep(sleep_for)
                backoff = min(backoff * 2, 60.0)

    # ---- helpers ----
    async def wait_connected(self, timeout: float = 10.0) -> None:
        await asyncio.wait_for(self._connected.wait(), timeout)

    @property
    def is_connected(self) -> bool:
        return self._connected.is_set()
