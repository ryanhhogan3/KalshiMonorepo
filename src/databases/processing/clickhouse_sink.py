# src/databases/processing/clickhouse_sink.py
import asyncio, os
from datetime import datetime, timezone
import urllib.parse as up
import clickhouse_connect
from .clickhouse_bootstrap import ensure_clickhouse_schema
from .clickhouse_health import assert_clickhouse_ready


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

        # column orders to keep ClickHouse happy
        self._cols_ev = ["type","sid","seq","market_id","market_ticker","side","price","qty","ts","ingest_ts"]
        self._cols_lt = ["market_id","market_ticker","side","price","size","ts","ingest_ts"]


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

        self.client = clickhouse_connect.get_client(...)

        # Ensure schema exists and matches what this sink expects
        ensure_clickhouse_schema(self.client, db=db)

        # Fail-fast if ClickHouse is in a bad state (e.g., broken parts)
        max_broken = int(os.getenv("CH_MAX_BROKEN_PARTS", "0"))
        assert_clickhouse_ready(
            self.client,
            db=db,
            tables=("orderbook_events", "latest_levels"),
            max_broken_parts=max_broken,
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

    def _insert_compat(self, table: str, buf: dict, cols: list[str]):
        """
        Handle different clickhouse-connect versions:
        1) Try column-oriented kw 'columnar' (newer)
        2) Try 'column_oriented' (older)
        3) Fallback to row-oriented data
        """
        data_cols = [buf[c] for c in cols]           # list of columns
        try:
            # newer versions
            self.client.insert(
                table, data_cols,
                column_names=cols,
                columnar=True,
                database=self.client.database,
            )
            return
        except TypeError:
            pass
        try:
            # some older versions used 'column_oriented'
            self.client.insert(
                table, data_cols,
                column_names=cols,
                column_oriented=True,
                database=self.client.database,
            )
            return
        except TypeError:
            pass
        # Oldest versions: only row-oriented supported
        rows = list(zip(*data_cols))                 # transpose to rows
        self.client.insert(
            table, rows,
            column_names=cols,
            database=self.client.database,
        )


    async def flush(self):
        # events buffer
        if self._buf_ev["type"]:
            b = self._buf_ev
            self._buf_ev = self._new_ev()
            self._t = asyncio.get_event_loop().time()
            try:
                self._insert_compat(self.table_events, b, self._cols_ev)
            except Exception as e:
                print(f"[CH][events] insert failed: {e!r}; rows={len(b['type'])}")
                raise

        # latest_levels buffer
        if self._buf_lt["market_id"]:
            b = self._buf_lt
            self._buf_lt = self._new_lt()
            try:
                self._insert_compat(self.table_latest, b, self._cols_lt)
            except Exception as e:
                print(f"[CH][latest] insert failed: {e!r}; rows={len(b['market_id'])}")
                raise

