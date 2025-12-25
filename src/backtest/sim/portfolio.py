from __future__ import annotations

from dataclasses import dataclass
from typing import List
from backtest.sim.types import Fill


@dataclass
class Portfolio:
    cash: float = 0.0
    inventory: float = 0.0

    def apply_fill(self, f: Fill) -> None:
        if f.side == 'buy':
            self.inventory += f.size
            self.cash -= f.price * f.size
        else:
            self.inventory -= f.size
            self.cash += f.price * f.size

    def equity(self, mid_price: float) -> float:
        return self.cash + self.inventory * mid_price
