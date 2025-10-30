import asyncio
import os
import time
import base64
from dotenv import load_dotenv
import websockets

from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.primitives.asymmetric import padding

# Load .env variables
load_dotenv()
API_KEY_ID = os.getenv("DEMO_KEYID")
PRIVATE_KEY_FILE = os.getenv("DEMO_KEYFILE")

# --- Step 1: Load the RSA private key ---
with open(PRIVATE_KEY_FILE, "rb") as f:
    private_key = serialization.load_pem_private_key(f.read(), password=None)

# --- Step 2: Define constants ---
WS_URL = "wss://demo-api.kalshi.co/trade-api/ws/v2"
METHOD = "GET"
PATH = "/trade-api/ws/v2"

# --- Step 3: Build timestamp + signature ---
timestamp = str(int(time.time() * 1000))
message_to_sign = (timestamp + METHOD + PATH).encode("utf-8")

signature = base64.b64encode(
    private_key.sign(
        message_to_sign,
        padding.PKCS1v15(),
        hashes.SHA256()
    )
).decode("utf-8")

# --- Step 4: Build authentication headers ---
auth_headers = {
    "KALSHI-ACCESS-KEY": API_KEY_ID,
    "KALSHI-ACCESS-TIMESTAMP": timestamp,
    "KALSHI-ACCESS-SIGNATURE": signature,
}

# --- Step 5: Connect ---
async def connect():
    print("Connecting to Kalshi demo WebSocket…")

    async with websockets.connect(WS_URL, additional_headers=auth_headers) as ws:
        print("✅ Connected successfully!\n")

        # Example: ask which subscriptions exist
        await ws.send('{"op": "subscriptions.list"}')
        response = await ws.recv()
        print("Server response:", response)

        # Keep listening (you can break this with Ctrl+C)
        async for message in ws:
            print("Received:", message)

asyncio.run(connect())
