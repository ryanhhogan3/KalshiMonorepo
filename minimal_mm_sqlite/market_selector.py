from __future__ import annotations

import os
import time
from dataclasses import dataclass
from typing import Set


@dataclass
class MarketSelector:
    file_path: str
    reload_secs: int = 5

    _last_reload_ms: int = 0
    _markets: Set[str] = None  # type: ignore

    def __post_init__(self) -> None:
        if self._markets is None:
            self._markets = set()

    def get_markets(self) -> Set[str]:
        now = int(time.time() * 1000)
        if self._last_reload_ms and (now - self._last_reload_ms) < self.reload_secs * 1000:
            return set(self._markets)

        self._last_reload_ms = now
        self._markets = self._read_file(self.file_path)
        return set(self._markets)

    @staticmethod
    def _read_file(path: str) -> Set[str]:
        out: Set[str] = set()
        if not os.path.exists(path):
            return out
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                s = line.strip()
                if not s or s.startswith("#"):
                    continue
                out.add(s)
        return out
