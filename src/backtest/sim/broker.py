from __future__ import annotations

import time
from typing import Dict, List
from dataclasses import asdict
from backtest.sim.types import OrderAction, Order


class Broker:
    def __init__(self):
        self.open_orders: Dict[str, Order] = {}
        self.n_place = 0
        self.n_cancel = 0
        self.n_modify = 0
        self._next_id = 1

    def _gen_id(self) -> str:
        oid = f"o{self._next_id}"
        self._next_id += 1
        return oid

    def place(self, action: OrderAction, ts_ms: int = None) -> Order:
        if ts_ms is None:
            ts_ms = int(time.time() * 1000)
        oid = action.order_id or self._gen_id()
        o = Order(order_id=oid, side=action.side, price=action.price, size=action.size, status="open", created_ts_ms=ts_ms, updated_ts_ms=ts_ms)
        self.open_orders[oid] = o
        self.n_place += 1
        return o

    def cancel(self, order_id: str, ts_ms: int = None) -> bool:
        if ts_ms is None:
            ts_ms = int(time.time() * 1000)
        if order_id in self.open_orders:
            o = self.open_orders.pop(order_id)
            o.status = "canceled"
            o.updated_ts_ms = ts_ms
            self.n_cancel += 1
            return True
        return False

    def modify(self, order_id: str, new_price: float, new_size: float, ts_ms: int = None) -> bool:
        if ts_ms is None:
            ts_ms = int(time.time() * 1000)
        if order_id in self.open_orders:
            o = self.open_orders[order_id]
            o.price = new_price
            o.size = new_size
            o.updated_ts_ms = ts_ms
            self.n_modify += 1
            return True
        return False

    def apply_action(self, action: OrderAction, ts_ms: int = None) -> Order:
        if action.kind == "place":
            return self.place(action, ts_ms=ts_ms)
        elif action.kind == "cancel":
            self.cancel(action.order_id or "", ts_ms=ts_ms)
            return None
        elif action.kind == "modify":
            self.modify(action.order_id or "", action.price, action.size, ts_ms=ts_ms)
            return None
        else:
            raise ValueError("unknown action kind")

    def active_orders_by_side(self, side: str):
        return [o for o in self.open_orders.values() if o.side == side and o.status == "open"]

    def all_orders(self) -> List[Order]:
        return list(self.open_orders.values())
