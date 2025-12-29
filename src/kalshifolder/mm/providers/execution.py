import requests
import time
import logging
import base64
from typing import Optional
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

    def place_order(self, market_ticker: str, side: str, price: float, size: float, client_order_id: str):
        # OpenAPI: POST /portfolio/orders (CreateOrder)
        path = '/portfolio/orders'
        url = self._endpoint(path)
        # API expects fields like 'ticker' and 'count' in some variants; include both common names
        payload = {
            'market_ticker': market_ticker,
            'ticker': market_ticker,
            'side': side,
            'price': self.format_price_for_api(price),
            'price_cents': int(self.format_price_for_api(price) if isinstance(self.format_price_for_api(price), int) else round(self.format_price_for_api(price)*100)),
            'size': int(size),
            'count': int(size),
            'client_order_id': client_order_id,
        }
        body = requests.utils.json.dumps(payload)
        headers = self._signed_headers('POST', path, body)
        t0 = int(time.time() * 1000)
        try:
            r = requests.post(url, data=body, headers=headers, timeout=self.timeout)
            latency = int(time.time() * 1000) - t0
            status = 'ACK' if r.status_code in (200, 201) or r.status_code < 300 else 'REJECT'
            exchange_id = None
            try:
                j = r.json()
                # CreateOrder may return {"order": {...}} or the order object directly
                order_obj = None
                if isinstance(j, dict):
                    order_obj = j.get('order') or j.get('orders') or j
                else:
                    order_obj = j
                if isinstance(order_obj, list) and order_obj:
                    order_obj = order_obj[0]
                if isinstance(order_obj, dict):
                    exchange_id = order_obj.get('order_id') or order_obj.get('id') or order_obj.get('orderId') or order_obj.get('exchange_order_id')
            except Exception:
                exchange_id = None
            return {'status': status, 'latency_ms': latency, 'raw': r.text, 'code': r.status_code, 'exchange_order_id': exchange_id}
        except Exception as e:
            logger.exception('place_order failed')
            latency = int(time.time() * 1000) - t0
            return {'status': 'ERROR', 'latency_ms': latency, 'raw': str(e)}

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
            if isinstance(j, dict):
                return j.get('orders') or j.get('order') or []
            return j
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
            if isinstance(j, dict):
                return j.get('positions') or j.get('market_positions') or j
            return j
        except Exception:
            logger.exception('get_positions failed')
            return []

    def get_fills(self, since_ts_ms: int = 0):
        path = '/portfolio/fills'
        url = self._endpoint(path)
        params = {'min_ts': since_ts_ms} if since_ts_ms else {}
        headers = self._signed_headers('GET', path, '')
        try:
            r = requests.get(url, params=params, headers=headers, timeout=self.timeout)
            r.raise_for_status()
            j = r.json()
            if isinstance(j, dict):
                return j.get('fills') or j
            return j
        except Exception:
            logger.exception('get_fills failed')
            return []
