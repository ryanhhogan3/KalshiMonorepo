from collections import defaultdict

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
        new_size = self.levels[pc] + d
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

    def size_at(self, side: str, price_cents: int) -> int:
        b = self.yes if side == "yes" else self.no
        return b.levels.get(int(price_cents), 0)
