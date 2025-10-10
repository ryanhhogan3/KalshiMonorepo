import websockets
import asyncio
from auth import env_auth_headers 

# WebSocket URL
ws_url = "wss://demo-api.kalshi.co/trade-api/ws/v2"  # Demo environment

# Generate authentication headers (see API Keys documentation)
auth_headers = {
    "KALSHI-ACCESS-KEY": env_auth_headers["KALSHI-ACCESS-KEY"],
    "KALSHI-ACCESS-SIGNATURE": env_auth_headers["KALSHI-ACCESS-SIGNATURE"],
    "KALSHI-ACCESS-TIMESTAMP": env_auth_headers["KALSHI-ACCESS-TIMESTAMP"]
}

# Connect with authentication
async def connect():
    async with websockets.connect(ws_url, additional_headers=auth_headers) as websocket:
        print("Connected to Kalshi WebSocket")

        # Connection is now established
        # You can start sending and receiving messages

        # Listen for messages
        async for message in websocket:
            print(f"Received: {message}")

# Run the connection
asyncio.run(connect())  