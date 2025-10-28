from dotenv import load_dotenv
import os
from pathlib import Path
import sys
from cryptography.hazmat.primitives import serialization

# Ensure `src/` is on sys.path so `kalshi` package (under src/) can be imported
repo_root = Path(__file__).resolve().parent
src_path = str(repo_root / "src")
if src_path not in sys.path:
    sys.path.insert(0, src_path)

from kalshi.api.client import KalshiHttpClient, Environment

load_dotenv()

env = Environment.DEMO
KEYID = os.getenv('DEMO_KEYID') if env == Environment.DEMO else os.getenv('PROD_KEYID')
KEYFILE = os.getenv('DEMO_KEYFILE') if env == Environment.DEMO else os.getenv('PROD_KEYFILE')

if not KEYID or not KEYFILE:
    raise SystemExit("Missing DEMO_KEYID or DEMO_KEYFILE in .env")

with open(KEYFILE, "rb") as f:
    private_key = serialization.load_pem_private_key(f.read(), password=None)

client = KalshiHttpClient(key_id=KEYID, private_key=private_key, environment=env)
print("Calling exchange status...")
print(client.get_exchange_status())