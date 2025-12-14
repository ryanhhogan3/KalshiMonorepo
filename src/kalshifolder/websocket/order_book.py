from collections import defaultdict, deque
import time

# Tolerance and reject-window settings
NEG_CLAMP_TOLERANCE = 1    # treat tiny negative overshoots (<= this) as 0
REJECT_WINDOW_SECONDS = 30.0
REJECT_THRESHOLD = 5

class SideBook:
    def __init__(self):
        self.levels = defaultdict(int)  # price_cents -> size

    def clear_and_load(self, pairs):
        self.levels.clear()
        for price_cents, size in pairs:
            if size > 0:
                self.levels[int(price_cents)] = int(size)

    def apply_delta(self, price_cents: int, delta: int) -> int | None:
        pc = int(price_cents); d = int(delta)
        curr = int(self.levels.get(pc, 0))
        new_size = curr + d

        # Deleting a non-existent level: treat as already zero and ignore
        if new_size < 0 and curr == 0:
            return 0

        # Small negative overshoot: clamp to zero (don't treat as hard reject)
        if new_size < 0 and abs(new_size) <= NEG_CLAMP_TOLERANCE:
            # ensure level removed
            self.levels.pop(pc, None)
            return 0

        # Hard negative -> signal reject
        if new_size < 0:
            return None

        if new_size == 0:
            self.levels.pop(pc, None)
        else:
            self.levels[pc] = new_size
        return new_size

class OrderBook:
    def __init__(self, market_id: str, ticker: str):
        self.market_id = market_id
        self.ticker = ticker
        self.yes = SideBook()
        self.no  = SideBook()
        self.last_seq = None
        # simple reject tracking (timestamps in seconds since epoch)
        self._reject_ts = deque()

    def apply_snapshot(self, msg: dict, seq: int):
        self.yes.clear_and_load(msg.get("yes", []))
        self.no.clear_and_load(msg.get("no", []))
        self.last_seq = int(seq)

    def apply_delta(self, side: str, price: int, delta: int, seq: int) -> int | None:
        if self.last_seq is not None and int(seq) != self.last_seq + 1:
            return None
        book = self.yes if side == "yes" else self.no
        new_size = book.apply_delta(price, delta)
        if new_size is None:
            return None
        self.last_seq = int(seq)
        return new_size

    def record_reject(self, now_ts: float | None = None) -> bool:
        """Record a reject event for this book. Return True when rejects
        within the rolling window reach REJECT_THRESHOLD (i.e., it's time
        to request a resnapshot).
        """
        now = float(now_ts if now_ts is not None else time.time())
        self._reject_ts.append(now)
        # prune old
        cutoff = now - REJECT_WINDOW_SECONDS
        while self._reject_ts and self._reject_ts[0] < cutoff:
            self._reject_ts.popleft()
        return len(self._reject_ts) >= REJECT_THRESHOLD

    def size_at(self, side: str, price_cents: int) -> int:
        b = self.yes if side == "yes" else self.no
        return b.levels.get(int(price_cents), 0)
