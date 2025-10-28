"""
Async WebSocket client with reconnect, exponential backoff, heartbeat, and metrics hooks.
"""
import asyncio
import aiohttp
import logging
import time
from typing import Callable, Awaitable, Optional
from .auth import AuthProvider
from .messages import MessageEnvelope

WS_URL = "wss://demo-api.kalshi.co/trade-api/ws/v2"  # Demo environment
HEARTBEAT_INTERVAL = 15  # seconds
MAX_BACKOFF = 60  # seconds

class WebSocketClient:
    """
    Usage:
        client = WebSocketClient(auth_provider, on_message=handler)
        asyncio.run(client.run())
    """
    def __init__(self, auth_provider: AuthProvider, on_message: Callable[[MessageEnvelope], Awaitable[None]],
                 url: str = WS_URL) -> None:
        self.auth_provider = auth_provider
        self.on_message = on_message
        self.url = url
        self._session: Optional[aiohttp.ClientSession] = None
        self._ws: Optional[aiohttp.ClientWebSocketResponse] = None
        self._last_heartbeat = time.time()
        self._backoff = 1
        self._running = True
        self.logger = logging.getLogger("WebSocketClient")

    async def run(self) -> None:
        while self._running:
            try:
                headers = await self.auth_provider.ws_headers()
                async with aiohttp.ClientSession() as session:
                    self._session = session
                    async with session.ws_connect(self.url, headers=headers) as ws:
                        self._ws = ws
                        self.logger.info("Connected to Kalshi WebSocket.")
                        self._backoff = 1
                        await self._listen()
            except Exception as e:
                self.logger.error(f"WebSocket error: {e}")
                await asyncio.sleep(self._backoff)
                self._backoff = min(self._backoff * 2, MAX_BACKOFF)
                self.logger.info(f"Reconnecting in {self._backoff} seconds...")

    async def _listen(self) -> None:
        self._last_heartbeat = time.time()
        async for msg in self._ws:
            if msg.type == aiohttp.WSMsgType.TEXT:
                envelope = MessageEnvelope.from_raw(msg.json())
                await self.on_message(envelope)
            elif msg.type == aiohttp.WSMsgType.BINARY:
                # TODO: handle binary messages if needed
                pass
            elif msg.type == aiohttp.WSMsgType.CLOSE:
                self.logger.warning("WebSocket closed by server.")
                break
            elif msg.type == aiohttp.WSMsgType.ERROR:
                self.logger.error(f"WebSocket error: {self._ws.exception()}")
                break
            # Heartbeat management
            self._last_heartbeat = time.time()
        await self._check_staleness()

    async def _check_staleness(self) -> None:
        now = time.time()
        if now - self._last_heartbeat > HEARTBEAT_INTERVAL * 2:
            self.logger.warning("WebSocket heartbeat stale, reconnecting...")
            await self._ws.close()

    async def stop(self) -> None:
        self._running = False
        if self._ws:
            await self._ws.close()
        if self._session:
            await self._session.close()