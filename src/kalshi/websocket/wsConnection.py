# import asyncio, os, time, base64, ssl, certifi, websockets
# from dotenv import load_dotenv
# from cryptography.hazmat.primitives import serialization, hashes
# from cryptography.hazmat.primitives.asymmetric import padding

# load_dotenv()
# KEY_ID  = os.getenv("PROD_KEYID").strip()
# KEYFILE = os.getenv("PROD_KEYFILE").strip()

# WS_URL  = "wss://api.elections.kalshi.com/trade-api/ws/v2"
# METHOD, PATH = "GET", "/trade-api/ws/v2"

# with open(KEYFILE, "rb") as f:
#     private_key = serialization.load_pem_private_key(f.read(), password=None)

# def sign_headers():
#     ts = str(int(time.time() * 1000))
#     msg = (ts + METHOD + PATH).encode("utf-8")
#     sig = base64.b64encode(
#         private_key.sign(
#             msg,
#             padding.PSS(
#                 mgf=padding.MGF1(hashes.SHA256()),
#                 salt_length=padding.PSS.DIGEST_LENGTH,
#             ),
#             hashes.SHA256(),
#         )
#     ).decode("utf-8")
#     return [
#         ("KALSHI-ACCESS-KEY", KEY_ID),
#         ("KALSHI-ACCESS-TIMESTAMP", ts),
#         ("KALSHI-ACCESS-SIGNATURE", sig),
#     ]

# ssl_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
# ssl_ctx.load_verify_locations(certifi.where())

# async def main():
#     print("Connecting to Kalshi production WebSocket…")
#     headers = sign_headers()                 # fresh timestamp + signature
#     async with websockets.connect(
#         WS_URL,
#         additional_headers=headers,
#         ssl=ssl_ctx,
#         server_hostname="api.elections.kalshi.com",
#         open_timeout=25,
#         ping_interval=20,
#         ping_timeout=20,
#     ) as ws:
#         print("✅ Connected to production!\n")
#         await ws.send('{"op":"subscriptions.list"}')
#         print(await ws.recv())

#         # Example: subscribe to an orderbook stream
#         # await ws.send('{"op":"subscribe","channel":"orderbook","tickers":["FEDFUNDS-DEC25"]}')
#         # print(await ws.recv())

# asyncio.run(main())
