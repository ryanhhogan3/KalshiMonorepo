from __future__ import annotations

import json
import time
from typing import Any, Dict


def now_ms() -> int:
    return int(time.time() * 1000)


def json_msg(obj: Dict[str, Any]) -> str:
    return json.dumps(obj, separators=(",", ":"), ensure_ascii=False)


def clamp_cents(c: int) -> int:
    return max(1, min(99, int(c)))
