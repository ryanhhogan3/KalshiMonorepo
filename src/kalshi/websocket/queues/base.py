"""
Base sink protocol for message ingestion queues.
"""
import abc
from typing import Any

class Sink(abc.ABC):
    """
    Abstract base class for message sinks.
    """
    @abc.abstractmethod
    async def push(self, item: Any) -> None:
        """Push a normalized message to the sink."""
        pass
