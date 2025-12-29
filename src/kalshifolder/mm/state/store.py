from typing import Dict
from .models import EngineState, MarketRuntimeState


class EngineStateStore:
    def __init__(self, engine_state: EngineState):
        self._state = engine_state

    def get_market(self, ticker: str) -> MarketRuntimeState:
        if ticker not in self._state.markets:
            self._state.markets[ticker] = MarketRuntimeState(ticker=ticker)
        return self._state.markets[ticker]

    def set_kill_switch(self, val: bool):
        self._state.kill_switch = val

    def to_dict(self):
        return {
            'instance_id': self._state.instance_id,
            'version': self._state.version,
            'kill_switch': self._state.kill_switch,
        }
