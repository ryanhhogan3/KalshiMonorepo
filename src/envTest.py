import requests, time, base64, os
from dotenv import load_dotenv
from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.primitives.asymmetric import padding

load_dotenv()
KEY_ID  = (os.getenv("DEMO_KEYID") or "").strip()
KEYPATH = (os.getenv("DEMO_KEYFILE") or "").strip()

HOST = "https://demo-api.kalshi.co"
METHOD = "GET"
PATH   = "/trade-api/v2/me"   # requires auth

priv = serialization.load_pem_private_key(open(KEYPATH, "rb").read(), password=None)
ts  = str(int(time.time() * 1000))
msg = (ts + METHOD + PATH).encode("utf-8")
sig = base64.b64encode(priv.sign(msg, padding.PKCS1v15(), hashes.SHA256())).decode("utf-8")

headers = {
    "KALSHI-ACCESS-KEY": KEY_ID,
    "KALSHI-ACCESS-TIMESTAMP": ts,
    "KALSHI-ACCESS-SIGNATURE": sig,
}

r = requests.get(HOST + PATH, headers=headers, timeout=10)
print("status:", r.status_code, "| body:", r.text[:200])
