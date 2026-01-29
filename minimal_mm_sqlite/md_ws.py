from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from typing import Dict, List, Optional


def _ensure_repo_src_on_path() -> None:
    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    src_dir = os.path.join(repo_root, "src")
    if src_dir not in sys.path:
        sys.path.insert(0, src_dir)


_ensure_repo_src_on_path()

from kalshifolder.mm.providers.market_data_ws import WSMarketDataProvider

from .types import BBO


@dataclass
class WSMarketData:
    """Thin adapter around the repo WSMarketDataProvider."""

    provider: WSMarketDataProvider

    @classmethod
    def create(cls) -> "WSMarketData":
        return cls(provider=WSMarketDataProvider())

    async def start(self, markets: List[str]) -> None:
        await self.provider.start(markets)
        if hasattr(self.provider, "wait_for_initial_bbo"):
            try:
                await self.provider.wait_for_initial_bbo(markets, timeout_ms=5000)
            except Exception:
                pass

    async def stop(self) -> None:
        await self.provider.stop()

    def get_bbo_batch(self, markets: List[str]) -> Dict[str, Optional[BBO]]:
        raw = self.provider.get_batch_best_bid_ask(markets)
        out: Dict[str, Optional[BBO]] = {}
        for m in markets:
            v = raw.get(m)
            if not v:
                out[m] = None
                continue
            yes_bb = v.get("yes_bb_px")
            yes_ba = v.get("yes_ba_px")
            ingest_ts = v.get("ingest_ts_ms")

            def _to_cents(px) -> Optional[int]:
                if px is None:
                    return None
                try:
                    f = float(px)
                except Exception:
                    return None
                if f >= 1.0:
                    c = int(round(f))
                else:
                    c = int(round(f * 100.0))
                if c < 1 or c > 99:
                    return None
                return c

            out[m] = BBO(
                yes_bb_cents=_to_cents(yes_bb),
                yes_ba_cents=_to_cents(yes_ba),
                ingest_ts_ms=int(ingest_ts) if ingest_ts is not None else None,
            )
        return out
