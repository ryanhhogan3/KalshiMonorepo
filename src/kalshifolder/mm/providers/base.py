from typing import Dict, List


class BaseMarketDataProvider:
    """Abstract-ish provider interface for market data sources.

    Methods:
      - async start(): optional background tasks / connections
      - async stop(): optional cleanup
      - get_batch_best_bid_ask(markets) -> Dict[str, dict]: synchronous snapshot of BBOs
    """

    async def start(self) -> None:  # optional
        return None

    async def stop(self) -> None:  # optional
        return None

    def get_batch_best_bid_ask(self, markets: List[str]) -> Dict[str, dict]:
        raise NotImplementedError()
