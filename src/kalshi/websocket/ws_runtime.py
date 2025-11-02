# ws_runtime.py
import asyncio
import base64
import json
import os
import ssl
import time
from typing import Optional
from websockets.exceptions import ConnectionClosed

import certifi
import websockets
from dotenv import load_dotenv
from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.primitives.asymmetric import padding

load_dotenv()

DEFAULT_WS_URL = os.getenv("WS_URL", "wss://api.elections.kalshi.com/trade-api/ws/v2")
DEFAULT_HOST   = os.getenv("WS_HOSTNAME", "api.elections.kalshi.com")
METHOD, PATH   = "GET", "/trade-api/ws/v2"

PROD_KEYID  = (os.getenv("PROD_KEYID") or "").strip()
PROD_KEYFILE = (os.getenv("PROD_KEYFILE") or "").strip()

class KalshiWSRuntime:
    """
    Minimal connection manager for Kalshi WS:
      - RSA header signing per connect
      - auto-reconnect with backoff
      - reader -> asyncio.Queue
      - send() helper
      - clean shutdown
    """
    def __init__(
        self,
        key_id: Optional[str] = None,
        keyfile: Optional[str] = None,
        ws_url: str = DEFAULT_WS_URL,
        server_hostname: str = DEFAULT_HOST,
        open_timeout: float = 25.0,
        ping_interval: float = 20.0,
        ping_timeout: float = 20.0,
        queue_maxsize: int = 50_000,
        max_backoff: float = 30.0,
    ):
        self.key_id = (key_id or PROD_KEYID).strip()
        self.keyfile = (keyfile or PROD_KEYFILE).strip()
        if not self.key_id or not self.keyfile:
            raise RuntimeError("Missing PROD_KEYID or PROD_KEYFILE envs.")

        # Load private key once
        with open(self.keyfile, "rb") as f:
            self._private_key = serialization.load_pem_private_key(f.read(), password=None)

        self.ws_url = ws_url
        self.server_hostname = server_hostname
        self.open_timeout = open_timeout
        self.ping_interval = ping_interval
        self.ping_timeout = ping_timeout
        self.queue: asyncio.Queue = asyncio.Queue(maxsize=queue_maxsize)

        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        self._run_task: Optional[asyncio.Task] = None
        self._stop = asyncio.Event()
        self._max_backoff = max_backoff

        # SSL
        self._ssl_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        self._ssl_ctx.load_verify_locations(certifi.where())

    # ---------- signing & headers ----------
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
            ("KALSHI-ACCESS-KEY", self.key_id),
            ("KALSHI-ACCESS-TIMESTAMP", ts),
            ("KALSHI-ACCESS-SIGNATURE", sig),
        ]

    # ---------- public API ----------
    async def start(self):
        if self._run_task is None:
            self._stop.clear()
            self._run_task = asyncio.create_task(self._run_loop(), name="kalshi_ws_run")

    async def stop(self):
        self._stop.set()
        if self._ws:
            try:
                # Close if possible; some versions also have wait_closed()
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
            finally:
                self._run_task = None

    async def send(self, payload: str | dict):
        if isinstance(payload, dict):
            payload = json.dumps(payload)
        ws = self._ws
        if ws is None:
            raise RuntimeError("WebSocket not connected.")
        try:
            await ws.send(payload)
        except ConnectionClosed:
            # Normalize to a simple error your caller can handle/retry
            raise RuntimeError("WebSocket is closed or not ready.")

    # ---------- core run loop ----------
    async def _run_loop(self):
        backoff = 1.0
        while not self._stop.is_set():
            try:
                headers = self._signed_headers()  # fresh timestamp each connect
                async with websockets.connect(
                    self.ws_url,
                    additional_headers=headers,
                    ssl=self._ssl_ctx,
                    server_hostname=self.server_hostname,
                    open_timeout=self.open_timeout,
                    ping_interval=self.ping_interval,
                    ping_timeout=self.ping_timeout,
                    max_size=None,  # allow large frames
                ) as ws:
                    self._ws = ws
                    # Reset backoff after a successful connect
                    backoff = 1.0
                    # Optional: sanity call – list current subscriptions
                    await ws.send(json.dumps({"id": 1, "cmd": "list_subscriptions"}))
                    
                    # Read loop
                    async for msg in ws:
                        try:
                            # push raw string; consumer can json.loads()
                            await self.queue.put(msg)
                        except asyncio.CancelledError:
                            raise
                        except Exception:
                            # best-effort: skip bad puts to keep connection healthy
                            pass

            except asyncio.CancelledError:
                break
            except Exception:
                # connection failed or read errored; backoff and retry
                await asyncio.sleep(backoff)
                backoff = min(self._max_backoff, backoff * 2)
            finally:
                self._ws = None
        # drain: no-op

# Convenience subscribe helper (optional)
def build_orderbook_subscribe(tickers: list[str] | None = None, ticker: str | None = None, *, _id: int = 42) -> str:
    # Use either a single ticker or many
    params = {"channels": ["orderbook_delta"]}
    if ticker:
        params["market_ticker"] = ticker
    elif tickers:
        params["market_tickers"] = tickers
    else:
        raise ValueError("Provide ticker or tickers")
    return json.dumps({"id": _id, "cmd": "subscribe", "params": params})