from __future__ import annotations

import asyncio
import logging
import os
import uuid
from dataclasses import dataclass
from typing import Dict, Optional, Tuple

from dotenv import load_dotenv

from .config import MMConfig, load_config
from .db import SQLiteDB, SQLiteWriter, dumps
from .exec_client import KalshiExecClient, KalshiExecConfig
from .market_selector import MarketSelector
from .md_ws import WSMarketData
from .types import BBO, WorkingOrder
from .util import clamp_cents, now_ms, json_msg

log = logging.getLogger(__name__)

ENGINE_TAG = "MMSQL"  # client_order_id prefix


def _uid() -> str:
    return uuid.uuid4().hex


def _client_order_id(side: str) -> str:
    # keep short, deterministic-ish
    return f"{ENGINE_TAG}:{side}:{_uid()[:12]}"


def _parse_position(positions: list[dict], ticker: str) -> float:
    for p in positions:
        if (p.get("ticker") or p.get("market_ticker")) == ticker:
            try:
                return float(p.get("position") or p.get("pos") or p.get("count") or 0.0)
            except Exception:
                return 0.0
    return 0.0


def _extract_reject_reason(raw: str) -> str:
    if not raw:
        return ""
    try:
        import json

        j = json.loads(raw) if raw.lstrip().startswith("{") else None
        if isinstance(j, dict):
            err = j.get("error") or {}
            if isinstance(err, dict):
                return str(err.get("code") or err.get("message") or err.get("details") or "")
    except Exception:
        pass
    return ""


@dataclass
class Engine:
    cfg: MMConfig
    db: SQLiteDB
    writer: SQLiteWriter
    md: WSMarketData
    selector: MarketSelector
    exec: Optional[KalshiExecClient]

    working: Dict[Tuple[str, str], WorkingOrder]
    _running: bool = False

    @classmethod
    def build(cls) -> "Engine":
        load_dotenv(dotenv_path=os.getenv("DOTENV_PATH", None))
        cfg = load_config()

        db = SQLiteDB(cfg.sqlite_path)
        db.ensure_schema()
        writer = SQLiteWriter(db)

        selector = MarketSelector(cfg.markets_file, cfg.markets_reload_secs)
        md = WSMarketData.create()

        exec_client: Optional[KalshiExecClient] = None
        if cfg.trading_enabled:
            key_id = (os.getenv("PROD_KEYID") or "").strip()
            keyfile = (os.getenv("PROD_KEYFILE") or "").strip()
            if not key_id or not keyfile:
                raise RuntimeError("Missing PROD_KEYID/PROD_KEYFILE")
            exec_client = KalshiExecClient(
                KalshiExecConfig(base_url=cfg.kalshi_base_url, key_id=key_id, private_key_path=keyfile)
            )

        return cls(
            cfg=cfg,
            db=db,
            writer=writer,
            md=md,
            selector=selector,
            exec=exec_client,
            working={},
        )

    async def start(self) -> None:
        self._running = True
        markets = sorted(self.selector.get_markets())
        await self.md.start(markets)
        log.info(json_msg({"event": "mm_start", "markets": markets, "trading_enabled": self.cfg.trading_enabled}))

    async def stop(self) -> None:
        self._running = False
        await self.md.stop()

    def _decision_allowed(self, *, bbo: BBO, md_age_ms: Optional[int], pos: float) -> Tuple[bool, str]:
        if self.cfg.kill_on_stale:
            if md_age_ms is None or md_age_ms > self.cfg.max_level_age_ms:
                return False, "STALE_MARKET"
        if abs(pos) > float(self.cfg.max_pos):
            # still allow run; we’ll just quote reduce-only side.
            return True, "OVER_CAP_REDUCE_ONLY"
        return True, ""

    def _targets(self, bbo: BBO) -> Optional[Tuple[int, int]]:
        if bbo.yes_bb_cents is None or bbo.yes_ba_cents is None:
            return None
        bb = bbo.yes_bb_cents
        ba = bbo.yes_ba_cents
        if ba < bb:
            return None
        mid = (bb + ba) / 2.0
        half = max(1, int(round(self.cfg.spread_cents / 2.0)))
        target_yes_bid = clamp_cents(int(round(mid - half)))
        target_yes_ask = clamp_cents(int(round(mid + half)))
        # BUY NO at price = 100 - YES_ASK
        target_no_bid = clamp_cents(100 - target_yes_ask)
        return target_yes_bid, target_no_bid

    async def run_forever(self) -> None:
        await self.start()
        try:
            while self._running:
                await self.run_once()
                await asyncio.sleep(self.cfg.poll_ms / 1000.0)
        finally:
            await self.stop()

    async def run_once(self) -> None:
        markets = sorted(self.selector.get_markets())
        if not markets:
            return

        batch = self.md.get_bbo_batch(markets)
        for m in markets:
            bbo = batch.get(m)
            if not bbo:
                continue

            md_age_ms = None
            if bbo.ingest_ts_ms is not None:
                md_age_ms = now_ms() - int(bbo.ingest_ts_ms)

            pos = 0.0
            if self.exec:
                pos = _parse_position(self.exec.get_positions(), m)
                self.writer.insert_position(ts_ms=now_ms(), market=m, pos=pos)

            allowed, reason = self._decision_allowed(bbo=bbo, md_age_ms=md_age_ms, pos=pos)
            targets = self._targets(bbo)
            if not targets:
                continue
            target_yes_bid_cents, target_no_bid_cents = targets

            decision_id = _uid()
            self.writer.insert_decision(
                {
                    "id": decision_id,
                    "ts_ms": now_ms(),
                    "market": m,
                    "yes_bb_cents": bbo.yes_bb_cents,
                    "yes_ba_cents": bbo.yes_ba_cents,
                    "md_age_ms": md_age_ms,
                    "allowed": 1 if allowed else 0,
                    "reason": reason,
                    "target_yes_bid_cents": target_yes_bid_cents,
                    "target_no_bid_cents": target_no_bid_cents,
                    "size": int(self.cfg.size),
                }
            )

            if not allowed:
                # cancel our own working orders
                await self._cancel_side(m, decision_id, "BID")
                await self._cancel_side(m, decision_id, "ASK")
                continue

            # reduce-only gating when over cap
            over_cap = abs(pos) > float(self.cfg.max_pos)
            allow_bid = True
            allow_ask = True
            if over_cap:
                # If pos>0 (long YES), BUY NO reduces exposure; if pos<0 (short YES), BUY YES reduces exposure.
                if pos > 0:
                    allow_bid = False
                elif pos < 0:
                    allow_ask = False

            await self._ensure_quote(
                market=m,
                decision_id=decision_id,
                side="BID",
                api_side="yes",
                price_cents=target_yes_bid_cents,
                allow=allow_bid,
            )
            await self._ensure_quote(
                market=m,
                decision_id=decision_id,
                side="ASK",
                api_side="no",
                price_cents=target_no_bid_cents,
                allow=allow_ask,
            )

    async def _cancel_side(self, market: str, decision_id: str, side: str) -> None:
        wo = self.working.get((market, side))
        if not wo:
            return

        action_id = _uid()
        self.writer.insert_action(
            {
                "id": action_id,
                "decision_id": decision_id,
                "ts_ms": now_ms(),
                "market": market,
                "action_type": "CANCEL",
                "side": side,
                "api_side": wo.api_side,
                "client_order_id": wo.client_order_id,
                "price_cents": wo.price_cents,
                "size": wo.size,
                "request_json": dumps({"exchange_order_id": wo.exchange_order_id}),
            }
        )

        if not self.exec:
            self.writer.insert_response(
                {
                    "action_id": action_id,
                    "ts_ms": now_ms(),
                    "market": market,
                    "client_order_id": wo.client_order_id,
                    "status": "SIMULATED",
                    "exchange_order_id": wo.exchange_order_id or "",
                    "reject_reason": "",
                    "latency_ms": 0,
                    "response_json": dumps({"simulated": True}),
                }
            )
            self.working.pop((market, side), None)
            return

        if not wo.exchange_order_id:
            # cannot cancel; drop local state
            self.writer.insert_response(
                {
                    "action_id": action_id,
                    "ts_ms": now_ms(),
                    "market": market,
                    "client_order_id": wo.client_order_id,
                    "status": "ERROR",
                    "exchange_order_id": "",
                    "reject_reason": "missing_exchange_order_id",
                    "latency_ms": 0,
                    "response_json": "",
                }
            )
            self.working.pop((market, side), None)
            return

        resp = await asyncio.to_thread(self.exec.cancel_order, exchange_order_id=wo.exchange_order_id)
        status = resp.get("status") or "ERROR"
        reject_reason = _extract_reject_reason(resp.get("raw") or "")
        self.writer.insert_response(
            {
                "action_id": action_id,
                "ts_ms": now_ms(),
                "market": market,
                "client_order_id": wo.client_order_id,
                "status": status,
                "exchange_order_id": wo.exchange_order_id,
                "reject_reason": reject_reason,
                "latency_ms": int(resp.get("latency_ms") or 0),
                "response_json": resp.get("raw") or "",
            }
        )
        self.working.pop((market, side), None)

    async def _ensure_quote(
        self,
        *,
        market: str,
        decision_id: str,
        side: str,
        api_side: str,
        price_cents: int,
        allow: bool,
    ) -> None:
        wo = self.working.get((market, side))
        now = now_ms()
        needs_replace = False
        if not allow:
            await self._cancel_side(market, decision_id, side)
            return

        if wo is None:
            needs_replace = True
        else:
            age_ms = now - wo.placed_ts_ms
            if age_ms > self.cfg.quote_refresh_ms:
                needs_replace = True
            elif abs(int(price_cents) - int(wo.price_cents)) >= self.cfg.min_reprice_cents:
                needs_replace = True

        if not needs_replace:
            return

        if wo is not None:
            await self._cancel_side(market, decision_id, side)

        client_order_id = _client_order_id(side)
        action_id = _uid()
        request = {
            "ticker": market,
            "client_order_id": client_order_id,
            "side": api_side,
            "action": "buy",
            "type": "limit",
            "price_cents": int(price_cents),
            "count": int(self.cfg.size),
        }
        self.writer.insert_action(
            {
                "id": action_id,
                "decision_id": decision_id,
                "ts_ms": now_ms(),
                "market": market,
                "action_type": "PLACE",
                "side": side,
                "api_side": api_side,
                "client_order_id": client_order_id,
                "price_cents": int(price_cents),
                "size": int(self.cfg.size),
                "request_json": dumps(request),
            }
        )

        if not self.exec:
            exch_id = f"SIMULATED:{client_order_id}"
            self.writer.insert_response(
                {
                    "action_id": action_id,
                    "ts_ms": now_ms(),
                    "market": market,
                    "client_order_id": client_order_id,
                    "status": "SIMULATED",
                    "exchange_order_id": exch_id,
                    "reject_reason": "",
                    "latency_ms": 0,
                    "response_json": dumps({"simulated": True}),
                }
            )
            self.working[(market, side)] = WorkingOrder(
                client_order_id=client_order_id,
                exchange_order_id=exch_id,
                side=side,
                api_side=api_side,
                price_cents=int(price_cents),
                size=int(self.cfg.size),
                placed_ts_ms=now,
            )
            return

        resp = await asyncio.to_thread(
            self.exec.place_order,
            ticker=market,
            side=api_side,
            action="buy",
            price_cents=int(price_cents),
            count=int(self.cfg.size),
            client_order_id=client_order_id,
        )
        status = resp.get("status") or "ERROR"
        exch_id = resp.get("exchange_order_id")
        reject_reason = _extract_reject_reason(resp.get("raw") or "")

        self.writer.insert_response(
            {
                "action_id": action_id,
                "ts_ms": now_ms(),
                "market": market,
                "client_order_id": client_order_id,
                "status": status,
                "exchange_order_id": exch_id or "",
                "reject_reason": reject_reason,
                "latency_ms": int(resp.get("latency_ms") or 0),
                "response_json": resp.get("raw") or "",
            }
        )

        if status == "ACK" and exch_id:
            self.working[(market, side)] = WorkingOrder(
                client_order_id=client_order_id,
                exchange_order_id=str(exch_id),
                side=side,
                api_side=api_side,
                price_cents=int(price_cents),
                size=int(self.cfg.size),
                placed_ts_ms=now,
            )
        else:
            # reject: do not keep local working order
            log.warning(json_msg({"event": "place_reject", "market": market, "side": side, "reason": reject_reason}))
