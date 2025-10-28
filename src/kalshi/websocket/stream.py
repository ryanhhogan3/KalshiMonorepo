"""
Async WebSocket stream utilities for Kalshi.
Usage example:

    import asyncio
    from kalshi.websocket.auth import AuthProvider
    from kalshi.websocket.reconnect import WebSocketClient
    from kalshi.websocket.stream import subscribe_to_orderbook

    async def on_message(envelope):
        # process envelope
        pass

    auth = AuthProvider(api_key="...", rsa_pem="...")
    client = WebSocketClient(auth, on_message)
    await client.run()

    # After connection, subscribe to orderbooks:
    await subscribe_to_orderbook(client._ws, ["TICKER1", "TICKER2"])
"""
import json
import logging
from typing import List, Any
import aiohttp

async def subscribe_to_ticker(ws: aiohttp.ClientWebSocketResponse) -> None:
    """Subscribe to ticker updates."""
    subscription = {
        "id": 1,
        "cmd": "subscribe",
        "params": {
            "channels": ["ticker"]
        }
    }
    await ws.send_json(subscription)
    logging.info("Subscribed to ticker channel.")

async def subscribe_to_orderbook(ws: aiohttp.ClientWebSocketResponse, market_tickers: List[str]) -> None:
    """Subscribe to orderbook updates for specific markets."""
    subscription = {
        "id": 2,
        "cmd": "subscribe",
        "params": {
            "channels": ["orderbook_delta"],
            "market_tickers": market_tickers
        }
    }
    await ws.send_json(subscription)
    logging.info(f"Subscribed to orderbook_delta for: {market_tickers}")