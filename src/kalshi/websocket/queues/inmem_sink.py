"""
In-memory asyncio queue sink for message ingestion.
"""
import asyncio
import logging
from typing import Any
from .base import Sink

class InMemorySink(Sink):
    def __init__(self, maxsize: int = 1000) -> None:
        self.queue = asyncio.Queue(maxsize=maxsize)
        self.logger = logging.getLogger("InMemorySink")

    async def push(self, item: Any) -> None:
        try:
            await self.queue.put(item)
            self.logger.debug(f"Item pushed to in-memory sink: {item}")
        except Exception as e:
            self.logger.error(f"Failed to push item: {e}")

    async def pop(self) -> Any:
        return await self.queue.get()
