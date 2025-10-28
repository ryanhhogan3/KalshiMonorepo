"""
Redis Streams sink for message ingestion (optional, degrades gracefully).
"""
import logging
from typing import Any
from .base import Sink

try:
    import aioredis
    HAS_REDIS = True
except ImportError:
    HAS_REDIS = False

class RedisSink(Sink):
    def __init__(self, stream_name: str = "kalshi_stream", redis_url: str = "redis://localhost") -> None:
        self.stream_name = stream_name
        self.redis_url = redis_url
        self.logger = logging.getLogger("RedisSink")
        self._redis = None
        if not HAS_REDIS:
            self.logger.warning("aioredis not installed, RedisSink will not function.")

    async def connect(self) -> None:
        if not HAS_REDIS:
            return
        self._redis = await aioredis.create_redis_pool(self.redis_url)
        self.logger.info(f"Connected to Redis at {self.redis_url}")

    async def push(self, item: Any) -> None:
        if not HAS_REDIS or not self._redis:
            self.logger.error("RedisSink not available.")
            return
        try:
            await self._redis.xadd(self.stream_name, {"data": str(item)})
            self.logger.debug(f"Item pushed to Redis stream: {item}")
        except Exception as e:
            self.logger.error(f"Failed to push item to Redis: {e}")

    async def close(self) -> None:
        if self._redis:
            self._redis.close()
            await self._redis.wait_closed()
            self.logger.info("Redis connection closed.")
