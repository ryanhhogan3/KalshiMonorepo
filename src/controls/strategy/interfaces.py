# Strategy protocol
from typing import Protocol

class IStrategy(Protocol):
    def generate_quote(self, market_state):
        ...
