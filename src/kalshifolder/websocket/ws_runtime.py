# src/kalshi/websocket/ws_runtime.py
import asyncio, base64, json, os, ssl, time, random, logging
from typing import Dict, Optional, Set

import certifi
import websockets
from websockets.exceptions import ConnectionClosed, ConnectionClosedError
from dotenv import load_dotenv
from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.primitives.asymmetric import padding

log = logging.getLogger(__name__)

load_dotenv()

DEFAULT_WS_URL = os.getenv("WS_URL", "wss://api.elections.kalshi.com/trade-api/ws/v2")
DEFAULT_HOST   = os.getenv("WS_HOSTNAME", "api.elections.kalshi.com")
METHOD, PATH   = "GET", "/trade-api/ws/v2"

KEYID   = os.getenv("PROD_KEYID")   or os.getenv("KALSHI_KEY_ID") or os.getenv("PROD_KEY_ID")
KEYFILE = os.getenv("PROD_KEYFILE") or os.getenv("KALSHI_PRIVATE_KEY_PATH") or os.getenv("PROD_KEYFILE_PATH") or "/run/secrets/prod_keys.pem"

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
        self._connected = asyncio.Event()

        # IMPORTANT: Never trust sid->ticker as authoritative, but we keep it for debugging
        self.sid_to_ticker: Dict[int, str] = {}

        self.verbose = bool(int(os.getenv("WS_VERBOSE", "1")))

        # Track markets for auto-resubscribe on reconnect (per-ticker semantics)
        self._subscribed_markets: Set[str] = set()
        self._resub_id_base: int = 20_000  # ids used for reconnect resubscribe calls

        # Track last WS message time (monotonic seconds)
        self._last_ws_message_monotonic: float = time.monotonic()

    # ---- signing ----
    def _signed_headers(self):
        ts = str(int(time.time() * 1000))
        msg = (ts + METHOD + PATH).encode("utf-8")
        sig = base64.b64encode(
            self._private_key.sign(
                msg,
                padding.PSS(
                    mgf=padding.MGF1(hashes.SHA256()),
                    salt_length=padding.PSS.DIGEST_LENGTH,
                ),
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
        if self._run_task and not self._run_task.done():
            return
        self._stop.clear()
        self._run_task = asyncio.create_task(
            self.run_forever(session_logger=session_logger),
            name="kalshi_ws_run",
        )

    async def stop(self) -> None:
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

    async def subscribe_market(self, ticker: str, *, _id: int) -> None:
        """Subscribe exactly one market ticker (prevents multi-ticker -> single sid collapse)."""
        ticker = (ticker or "").strip()
        if not ticker:
            return

        payload = {
            "id": _id,
            "cmd": "subscribe",
            "params": {
                "channels": ["orderbook_delta"],
                "market_tickers": [ticker],
                "snapshot": True,
            },
        }
        await self.send(payload)
        self._subscribed_markets.add(ticker)

    async def subscribe_markets(self, tickers: list[str], *, _id: int = 42) -> None:
        """
        Subscribe markets.
        IMPORTANT: we intentionally send ONE subscribe per ticker
        to avoid Kalshi bundling multiple tickers under a single sid.
        """
        tickers = [t.strip() for t in tickers if t and t.strip()]
        if not tickers:
            return

        if len(tickers) > 1:
            log.warning(
                "subscribe_markets_called_with_multiple_tickers_splitting",
                extra={"count": len(tickers), "id": _id},
            )

        for i, t in enumerate(tickers):
            await self.subscribe_market(t, _id=_id + i)
            await asyncio.sleep(0.05)

    async def _resubscribe_all(self) -> None:
        """Re-send subscribe commands for all tracked markets after a reconnect (one ticker per request)."""
        if not self._subscribed_markets:
            return

        tickers = sorted(self._subscribed_markets)
        base = self._resub_id_base
        self._resub_id_base += max(len(tickers), 1) + 1

        for i, t in enumerate(tickers):
            await self.subscribe_market(t, _id=base + i)
            await asyncio.sleep(0.05)

        log.info("resubscribed_markets", extra={"count": len(tickers)})

    # ---- loop helpers ----
    async def _run_once(self):
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

                # Resubscribe after reconnect (safe: one ticker per request)
                try:
                    await self._resubscribe_all()
                except Exception:
                    log.exception("_resubscribe_all_failed_on_connect")

                async for raw in ws:
                    try:
                        self._last_ws_message_monotonic = time.monotonic()
                    except Exception:
                        pass
                    await self.queue.put(raw)
        finally:
            self._ws = None
            self._connected.clear()

    async def run_forever(self, session_logger=None):
        backoff = 1.0
        while not self._stop.is_set():
            try:
                await self._run_once()
                if session_logger:
                    session_logger.warning("ws_run_once_exited_cleanly")
                else:
                    log.warning("ws_run_once_exited_cleanly")
                await asyncio.sleep(5)
                backoff = 1.0

            except ConnectionClosedError as e:
                sleep_for = backoff + random.uniform(0, backoff * 0.1)
                text = str(e)[:500]
                if session_logger:
                    session_logger.info(
                        "ws_connection_closed",
                        extra={"context": "ws_consumer", "reason": text, "reconnecting_in": sleep_for},
                    )
                else:
                    log.info(
                        "ws_connection_closed",
                        extra={"context": "ws_consumer", "reason": text, "reconnecting_in": sleep_for},
                    )

                if self._stop.is_set():
                    break

                await asyncio.sleep(sleep_for)
                backoff = min(backoff * 2, 60.0)

            except Exception as e:
                sleep_for = backoff + random.uniform(0, backoff * 0.1)
                text = str(e)[:500]
                if session_logger:
                    session_logger.exception(
                        "ws_error",
                        extra={"context": "ws_consumer", "error": text, "reconnecting_in": sleep_for},
                    )
                else:
                    log.exception(
                        "ws_error",
                        extra={"context": "ws_consumer", "error": text, "reconnecting_in": sleep_for},
                    )

                if self._stop.is_set():
                    break

                await asyncio.sleep(sleep_for)
                backoff = min(backoff * 2, 60.0)

        self._connected.clear()

    @property
    def last_ws_message_age(self) -> float:
        try:
            return time.monotonic() - self._last_ws_message_monotonic
        except Exception:
            return float("inf")

    async def wait_connected(self, timeout: float = 10.0) -> None:
        await asyncio.wait_for(self._connected.wait(), timeout)

    @property
    def is_connected(self) -> bool:
        return self._connected.is_set()
