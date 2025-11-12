import requests, time, base64, os
from dotenv import load_dotenv
from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.primitives.asymmetric import padding

load_dotenv()
KEY_ID  = os.getenv("PROD_KEYID").strip()
KEYFILE = os.getenv("PROD_KEYFILE").strip()

HOST = "https://api.elections.kalshi.com"
METHOD = "GET"
PATH   = "/trade-api/v2/portfolio/balance"  # ✅ valid endpoint

priv = serialization.load_pem_private_key(open(KEYFILE, "rb").read(), password=None)
ts  = str(int(time.time() * 1000))
msg = (ts + METHOD + PATH).encode("utf-8")
sig = base64.b64encode(
    priv.sign(
        msg,
        padding.PSS(
            mgf=padding.MGF1(hashes.SHA256()),
            salt_length=padding.PSS.DIGEST_LENGTH,
        ),
        hashes.SHA256(),
    )
).decode()

headers = {
    "KALSHI-ACCESS-KEY": KEY_ID,
    "KALSHI-ACCESS-TIMESTAMP": ts,
    "KALSHI-ACCESS-SIGNATURE": sig,
}

r = requests.get(HOST + PATH, headers=headers, timeout=10)
print("status:", r.status_code)
print("body:", r.text[:300])
