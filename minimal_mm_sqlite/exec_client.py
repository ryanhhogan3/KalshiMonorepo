from __future__ import annotations

import base64
import json
import logging
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional

import requests
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

log = logging.getLogger(__name__)

API_PREFIX = "/trade-api/v2"


@dataclass
class KalshiExecConfig:
    base_url: str
    key_id: str
    private_key_path: str
    timeout_s: float = 5.0


class KalshiExecClient:
    """Minimal REST execution client (place/cancel/open_orders/positions)."""

    def __init__(self, cfg: KalshiExecConfig):
        self._base = cfg.base_url.rstrip("/")
        self._key_id = (cfg.key_id or "").strip()
        self._timeout = float(cfg.timeout_s)
        with open(cfg.private_key_path, "rb") as f:
            self._pk = serialization.load_pem_private_key(f.read(), password=None)

    def _norm_path(self, path: str) -> str:
        p = (path or "").strip()
        if not p.startswith("/"):
            p = "/" + p
        if not p.startswith(API_PREFIX):
            p = API_PREFIX + p
        return p

    def _endpoint(self, path: str) -> str:
        p = self._norm_path(path)
        return f"{self._base}{p}"

    def _signed_headers(self, method: str, path: str) -> Dict[str, str]:
        p = self._norm_path(path)
        p_to_sign = p.split("?", 1)[0]
        ts = str(int(time.time() * 1000))
        msg = (ts + method.upper() + p_to_sign).encode("utf-8")
        sig = base64.b64encode(
            self._pk.sign(
                msg,
                padding.PSS(
                    mgf=padding.MGF1(hashes.SHA256()),
                    salt_length=padding.PSS.DIGEST_LENGTH,
                ),
                hashes.SHA256(),
            )
        ).decode("utf-8")
        return {
            "KALSHI-ACCESS-KEY": self._key_id,
            "KALSHI-ACCESS-TIMESTAMP": ts,
            "KALSHI-ACCESS-SIGNATURE": sig,
            "Content-Type": "application/json",
        }

    def place_order(
        self,
        *,
        ticker: str,
        side: str,  # yes|no
        action: str,  # buy|sell
        price_cents: int,
        count: int,
        client_order_id: str,
    ) -> Dict[str, Any]:
        path = "/portfolio/orders"
        url = self._endpoint(path)
        side_norm = "yes" if side.lower() in ("yes", "y") else "no"
        action_norm = (action or "buy").lower()

        payload: Dict[str, Any] = {
            "ticker": ticker,
            "client_order_id": client_order_id,
            "count": int(count),
            "side": side_norm,
            "action": action_norm,
            "type": "limit",
        }
        if side_norm == "yes":
            payload["yes_price"] = int(price_cents)
        else:
            payload["no_price"] = int(price_cents)

        body = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
        headers = self._signed_headers("POST", path)

        t0 = int(time.time() * 1000)
        try:
            r = requests.post(url, data=body, headers=headers, timeout=self._timeout)
            latency_ms = int(time.time() * 1000) - t0
            status = "ACK" if r.status_code < 300 else "REJECT"
            exch_id = None
            try:
                j = r.json()
                order_obj = j.get("order") if isinstance(j, dict) else None
                if isinstance(order_obj, dict):
                    exch_id = order_obj.get("order_id") or order_obj.get("id")
            except Exception:
                pass
            return {
                "status": status,
                "exchange_order_id": exch_id,
                "latency_ms": latency_ms,
                "code": r.status_code,
                "raw": r.text,
            }
        except Exception as e:
            latency_ms = int(time.time() * 1000) - t0
            return {"status": "ERROR", "exchange_order_id": None, "latency_ms": latency_ms, "raw": str(e)}

    def cancel_order(self, *, exchange_order_id: str) -> Dict[str, Any]:
        path = f"/portfolio/orders/{exchange_order_id}"
        url = self._endpoint(path)
        headers = self._signed_headers("DELETE", path)
        t0 = int(time.time() * 1000)
        try:
            r = requests.delete(url, headers=headers, timeout=self._timeout)
            latency_ms = int(time.time() * 1000) - t0
            status = "ACK" if r.status_code < 300 else "REJECT"
            return {
                "status": status,
                "exchange_order_id": exchange_order_id,
                "latency_ms": latency_ms,
                "code": r.status_code,
                "raw": r.text,
            }
        except Exception as e:
            latency_ms = int(time.time() * 1000) - t0
            return {"status": "ERROR", "exchange_order_id": exchange_order_id, "latency_ms": latency_ms, "raw": str(e)}

    def get_open_orders(self, *, ticker: Optional[str] = None, limit: int = 200) -> list[dict]:
        path = "/portfolio/orders"
        url = self._endpoint(path)
        params: Dict[str, Any] = {"status": "resting", "limit": int(limit)}
        if ticker:
            params["ticker"] = ticker
        headers = self._signed_headers("GET", path)
        try:
            r = requests.get(url, params=params, headers=headers, timeout=self._timeout)
            r.raise_for_status()
            j = r.json()
            if isinstance(j, dict):
                orders = j.get("orders") or []
            elif isinstance(j, list):
                orders = j
            else:
                orders = []
            return [o for o in orders if isinstance(o, dict)]
        except Exception:
            log.exception("get_open_orders failed")
            return []

    def get_positions(self) -> list[dict]:
        path = "/portfolio/positions"
        url = self._endpoint(path)
        headers = self._signed_headers("GET", path)
        try:
            r = requests.get(url, headers=headers, timeout=self._timeout)
            r.raise_for_status()
            j = r.json()
            if isinstance(j, dict):
                positions = j.get("positions") or j.get("market_positions") or []
            elif isinstance(j, list):
                positions = j
            else:
                positions = []
            return [p for p in positions if isinstance(p, dict)]
        except Exception:
            log.exception("get_positions failed")
            return []
