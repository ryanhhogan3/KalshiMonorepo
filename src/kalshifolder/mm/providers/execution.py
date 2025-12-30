import requests
import time
import logging
import base64
from typing import Optional
import json
from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.primitives.asymmetric import padding

logger = logging.getLogger(__name__)


class KalshiExecutionProvider:
    def __init__(self, base_url: str, key_id: Optional[str] = None, private_key_path: Optional[str] = None, timeout: int = 5):
        self.base_url = base_url.rstrip('/')
        self.key_id = key_id
        self.private_key_path = private_key_path
        self.timeout = timeout
        self.price_units = 'cents'
        self._private_key = None
        if self.private_key_path:
            try:
                with open(self.private_key_path, 'rb') as f:
                    self._private_key = serialization.load_pem_private_key(f.read(), password=None)
            except Exception:
                logger.exception('Failed to load private key')

    def _endpoint(self, path: str) -> str:
        return f"{self.base_url}{path}"

    def set_price_units(self, units: str):
        self.price_units = units

    def format_price_for_api(self, price_cents: int):
        if self.price_units == 'cents':
            return int(price_cents)
        # dollars
        return round(float(price_cents) / 100.0, 2)

    def parse_price_from_api(self, api_price):
        try:
            if self.price_units == 'cents':
                return int(api_price)
            return int(round(float(api_price) * 100))
        except Exception:
            return 0

    def _signed_headers(self, method: str, path: str, body: Optional[str] = ''):
        # Ensure the signed path matches the actual request path including any
        # base path in the configured base_url (for example '/trade-api' or '/trade-api/v2').
        from urllib.parse import urlparse
        base_path = urlparse(self.base_url).path.rstrip('/')
        # normalize incoming path to start with '/'
        if path and not path.startswith('/'):
            path = '/' + path
        if base_path and not path.startswith(base_path):
            # Prefix base_path (may include /v2 if provided in base_url)
            path = f"{base_path}{path}"

        ts = str(int(time.time() * 1000))
        msg = (ts + method + path + (body or '')).encode('utf-8')
        sig = ''
        if self._private_key:
            try:
                sig = base64.b64encode(
                    self._private_key.sign(
                        msg,
                        padding.PSS(
                            mgf=padding.MGF1(hashes.SHA256()),
                            salt_length=padding.PSS.DIGEST_LENGTH,
                        ),
                        hashes.SHA256(),
                    )
                ).decode('utf-8')
            except Exception:
                logger.exception('signing failed')
        headers = {}
        if self.key_id:
            headers['KALSHI-ACCESS-KEY'] = self.key_id
        headers['KALSHI-ACCESS-TIMESTAMP'] = ts
        if sig:
            headers['KALSHI-ACCESS-SIGNATURE'] = sig
        headers['Content-Type'] = 'application/json'
        return headers

    def place_order(self, market_ticker: str, side: str, price_cents: int, size: float, client_order_id: str, action: str = "buy"):
        path = "/portfolio/orders"
        url = self._endpoint(path)

        price_cents = int(price_cents)
        count = int(size)

        if count <= 0:
            return {"status": "ERROR", "latency_ms": 0, "raw": "count must be >= 1"}
        if price_cents <= 0 or price_cents >= 100:
            return {"status": "ERROR", "latency_ms": 0, "raw": "price_cents must be 1..99"}

        side_norm = "yes" if side and side.lower() in ("yes", "y") else "no"
        action_norm = action.lower()

        payload = {
            "ticker": market_ticker,
            "client_order_id": client_order_id,
            "count": count,
            "side": side_norm,
            "action": action_norm,
        }
        if side_norm == "yes":
            payload["yes_price"] = price_cents
        else:
            payload["no_price"] = price_cents

        body = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
        headers = self._signed_headers("POST", path, body)
        headers.setdefault("Content-Type", "application/json")

        t0 = int(time.time() * 1000)
        try:
            r = requests.post(url, data=body, headers=headers, timeout=self.timeout)
            latency = int(time.time() * 1000) - t0
            status = "ACK" if r.status_code < 300 else "REJECT"

            exchange_id = None
            try:
                j = r.json()
                order_obj = j.get("order") if isinstance(j, dict) else None
                if isinstance(order_obj, dict):
                    exchange_id = order_obj.get("order_id") or order_obj.get("id")
            except Exception:
                pass

            return {"status": status, "latency_ms": latency, "raw": r.text, "code": r.status_code, "exchange_order_id": exchange_id}
        except Exception as e:
            latency = int(time.time() * 1000) - t0
            return {"status": "ERROR", "latency_ms": latency, "raw": str(e)}

    def cancel_order(self, client_order_id: str):
        # OpenAPI: DELETE /portfolio/orders/{order_id}
        # The API expects the order id. Allow callers to pass client_order_id or exchange id.
        path = f'/portfolio/orders/{client_order_id}'
        url = self._endpoint(path)
        headers = self._signed_headers('DELETE', path, '')
        t0 = int(time.time() * 1000)
        try:
            r = requests.delete(url, headers=headers, timeout=self.timeout)
            latency = int(time.time() * 1000) - t0
            status = 'ACK' if r.status_code < 300 else 'REJECT'
            exchange_id = None
            try:
                j = r.json()
                order_obj = j.get('order') if isinstance(j, dict) else j
                if isinstance(order_obj, dict):
                    exchange_id = order_obj.get('order_id') or order_obj.get('id') or order_obj.get('orderId') or order_obj.get('exchange_order_id')
            except Exception:
                exchange_id = None
            return {'status': status, 'latency_ms': latency, 'raw': r.text, 'code': r.status_code, 'exchange_order_id': exchange_id}
        except Exception as e:
            logger.exception('cancel_order failed')
            latency = int(time.time() * 1000) - t0
            return {'status': 'ERROR', 'latency_ms': latency, 'raw': str(e)}

    def get_open_orders(self):
        path = '/portfolio/orders'
        url = self._endpoint(path)
        headers = self._signed_headers('GET', path, '')
        try:
            r = requests.get(url, headers=headers, timeout=self.timeout)
            r.raise_for_status()
            j = r.json()
            # normalize to list[dict]
            if isinstance(j, dict):
                orders = j.get('orders') or j.get('order') or []
                if isinstance(orders, list):
                    return [o for o in orders if isinstance(o, dict)]
                if isinstance(orders, dict):
                    return [orders]
                # unexpected type
                logger.error('get_open_orders unexpected payload type for orders: %s keys=%s', type(orders), list(j.keys()))
                return []
            if isinstance(j, list):
                return [o for o in j if isinstance(o, dict)]
            logger.error('get_open_orders unexpected JSON root type: %s', type(j))
            return []
        except Exception:
            logger.exception('get_open_orders failed')
            return []

    def get_positions(self):
        path = '/portfolio/positions'
        url = self._endpoint(path)
        headers = self._signed_headers('GET', path, '')
        try:
            r = requests.get(url, headers=headers, timeout=self.timeout)
            r.raise_for_status()
            j = r.json()
            # normalize to list[dict]
            if isinstance(j, dict):
                positions = j.get('positions') or j.get('market_positions') or []
                if isinstance(positions, list):
                    return [p for p in positions if isinstance(p, dict)]
                if isinstance(positions, dict):
                    return [positions]
                logger.error('get_positions unexpected payload type for positions: %s keys=%s', type(positions), list(j.keys()))
                return []
            if isinstance(j, list):
                return [p for p in j if isinstance(p, dict)]
            logger.error('get_positions unexpected JSON root type: %s', type(j))
            return []
        except Exception:
            logger.exception('get_positions failed')
            return []

    def get_fills(self, since_ts_ms: int = 0):
        path = '/portfolio/fills'
        url = self._endpoint(path)

        # API uses seconds. Convert incoming ms -> seconds.
        params = {'min_ts': int(since_ts_ms // 1000)} if since_ts_ms else {}

        # If signature includes querystring, include it in the signed path
        from urllib.parse import urlencode
        qs = f"?{urlencode(params)}" if params else ""
        signed_path = path + qs

        headers = self._signed_headers('GET', signed_path, '')
        try:
            r = requests.get(url, params=params, headers=headers, timeout=self.timeout)
            r.raise_for_status()
            j = r.json()

            # Normalize to list[dict]
            if isinstance(j, dict):
                fills = (
                    j.get('fills')
                    or j.get('fill')
                    or j.get('data')
                    or j.get('items')
                    or j.get('result')
                    or []
                )
                if isinstance(fills, list):
                    return [f for f in fills if isinstance(f, dict)]
                if isinstance(fills, dict):
                    return [fills]
                logger.error('get_fills unexpected fills type=%s keys=%s', type(fills), list(j.keys()))
                return []

            if isinstance(j, list):
                return [f for f in j if isinstance(f, dict)]

            logger.error('get_fills unexpected JSON root type=%s', type(j))
            return []
        except Exception:
            logger.exception('get_fills failed')
            return []
