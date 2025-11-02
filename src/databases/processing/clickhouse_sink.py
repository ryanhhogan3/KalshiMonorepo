# src/databases/processing/clickhouse_sink.py
import asyncio, os
from datetime import datetime, timezone
import urllib.parse as up
import clickhouse_connect


class ClickHouseSink:
    def __init__(self, *, batch_rows=2000, flush_secs=0.010):
        raw = os.getenv("CLICKHOUSE_URL", "http://localhost:8123")
        user = os.getenv("CLICKHOUSE_USER", "default")
        pwd  = os.getenv("CLICKHOUSE_PASSWORD", "")
        db   = os.getenv("CLICKHOUSE_DATABASE", "kalshi")

        # Parse http(s)://host:port from CLICKHOUSE_URL for older clients
        p = up.urlparse(raw)
        scheme = (p.scheme or "http").lower()
        host = p.hostname or "localhost"
        port = p.port or (8443 if scheme == "https" else 8123)

        # Build client using host/port (no url=)
        # interface http covers both http and https automatically
        self.client = clickhouse_connect.get_client(
            host=host,
            port=port,
            username=user,
            password=pwd,
            database=db,
            interface="http",
            secure=(scheme == "https"),
        )

        self.table_events = "orderbook_events"
        self.table_latest = "latest_levels"
        self.batch_rows = int(os.getenv("BATCH_ROWS", batch_rows))
        self.flush_secs = float(os.getenv("BATCH_FLUSH_SECS", flush_secs))

        self._buf_ev = self._new_ev()
        self._buf_lt = self._new_lt()
        self._t = asyncio.get_event_loop().time()
        self._lock = asyncio.Lock()

    def _new_ev(self):
        return {k: [] for k in ("type","sid","seq","market_id","market_ticker","side","price","qty","ts","ingest_ts")}
    def _new_lt(self):
        return {k: [] for k in ("market_id","market_ticker","side","price","size","ts","ingest_ts")}

    async def add_event(self, row: dict):
        async with self._lock:
            for k in self._buf_ev: self._buf_ev[k].append(row[k])
            await self._maybe_flush()

    async def upsert_latest(self, row: dict):
        async with self._lock:
            for k in self._buf_lt: self._buf_lt[k].append(row[k])
            await self._maybe_flush()

    async def _maybe_flush(self):
        now = asyncio.get_event_loop().time()
        if len(self._buf_ev["type"]) >= self.batch_rows or (now - self._t) >= self.flush_secs:
            await self.flush()

    async def flush(self):
        # Flush events
        if self._buf_ev["type"]:
            b = self._buf_ev
            self._buf_ev = self._new_ev()
            self._t = asyncio.get_event_loop().time()
            self.client.insert(self.table_events, b, database=self.client.database)
        # Flush latest_levels
        if self._buf_lt["market_id"]:
            b = self._buf_lt
            self._buf_lt = self._new_lt()
            self.client.insert(self.table_latest, b, database=self.client.database)
