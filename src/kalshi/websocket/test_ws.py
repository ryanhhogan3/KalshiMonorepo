import asyncio
import logging
from kalshi.websocket.auth import AuthProvider
from kalshi.websocket.reconnect import WebSocketClient
from kalshi.websocket.stream import subscribe_to_orderbook

logging.basicConfig(level=logging.INFO)

# Replace with your real API key and RSA PEM
API_KEY = "your_api_key"
RSA_PEM = "your_rsa_pem"

async def on_message(envelope):
    logging.info(f"Received envelope: {envelope}")

async def main():
    auth = AuthProvider(api_key=API_KEY, rsa_pem=RSA_PEM)
    client = WebSocketClient(auth, on_message)
    # Run client in background
    task = asyncio.create_task(client.run())
    # Wait for connection, then subscribe
    await asyncio.sleep(2)
    if client._ws:
        await subscribe_to_orderbook(client._ws, ["TICKER1", "TICKER2"])
    await asyncio.sleep(10)  # Run for 10 seconds
    await client.stop()
    await task

if __name__ == "__main__":
    # Run from project root with: python -m src.kalshi.websocket.test_ws
    asyncio.run(main())