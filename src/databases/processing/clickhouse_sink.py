# src/databases/processing/clickhouse_sink.py
import asyncio, os
from datetime import datetime, timezone
import urllib.parse as up
import clickhouse_connect
from clickhouse_connect.driver.exceptions import DatabaseError, OperationalError
import logging

from .clickhouse_bootstrap import ensure_clickhouse_schema
from .clickhouse_health import assert_clickhouse_ready

log = logging.getLogger(__name__)


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
        # client-side resource limits (per-query)
        client_max_mem = int(os.getenv("CLICKHOUSE_CLIENT_MAX_MEMORY_USAGE", 2_000_000_000))
        client_settings = {
            "max_memory_usage": client_max_mem,
            "use_uncompressed_cache": 0,
        }

        self.client = clickhouse_connect.get_client(
            host=host,
            port=port,
            username=user,
            password=pwd,
            database=db,
            interface="http",
            secure=(scheme == "https"),
            settings=client_settings,
        )
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
        # Newer, clearer config names
        self.flush_rows = int(os.getenv("FLUSH_ROWS", 5_000))
        self.flush_secs = float(os.getenv("BATCH_FLUSH_SECS", flush_secs))
        # Safety cap for in-memory buffer
        self.max_buffer_rows = int(os.getenv("MAX_BUFFER_ROWS", 50_000))

        # Event buffer is now a list of row dicts (bounded to avoid OOM)
        self._buffer_events: list[dict] = []
        # Keep columnar buffer for "latest_levels" as before
        self._buf_lt = self._new_lt()
        self._t = asyncio.get_event_loop().time()
        self._lock = asyncio.Lock()
        # Metrics for monitoring
        self.insert_failures = 0
        self.inserts_total = 0
        self.last_insert_ts = None

    def _new_ev(self):
        return {k: [] for k in ("type","sid","seq","market_id","market_ticker","side","price","qty","ts","ingest_ts")}
    def _new_lt(self):
        return {k: [] for k in ("market_id","market_ticker","side","price","size","ts","ingest_ts")}

    async def add_event(self, row: dict):
        async with self._lock:
            # append to event list buffer
            self._buffer_events.append(row)

            # enforce hard cap to avoid unbounded memory growth
            if len(self._buffer_events) > self.max_buffer_rows:
                overflow = len(self._buffer_events) - self.max_buffer_rows
                log.warning(
                    "buffer_overflow",
                    extra={"rows": len(self._buffer_events), "overflow": overflow},
                )
                # drop the oldest rows and keep the newest max_buffer_rows
                self._buffer_events = self._buffer_events[overflow:]

            await self._maybe_flush()

    async def upsert_latest(self, row: dict):
        async with self._lock:
            for k in self._buf_lt:
                self._buf_lt[k].append(row[k])
            await self._maybe_flush()

    async def _maybe_flush(self):
        now = asyncio.get_event_loop().time()
        # flush if we have reached configured row count, or enough time passed
        if len(self._buffer_events) >= self.flush_rows or (now - self._t) >= self.flush_secs:
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
        # events buffer (now row-oriented)
        if not self._buffer_events:
            # still attempt to flush latest_levels below
            pass
        else:
            batch = self._buffer_events
            self._buffer_events = []
            self._t = asyncio.get_event_loop().time()
            # try to insert rows with retry & safety; never raise to caller
            await self._insert_with_retry(self.table_events, batch, self._cols_ev)

        # latest_levels buffer (columnar)
        if self._buf_lt["market_id"]:
            b = self._buf_lt
            self._buf_lt = self._new_lt()
            try:
                self._insert_compat(self.table_latest, b, self._cols_lt)
            except Exception as e:
                log.exception("clickhouse_latest_insert_failed: rows=%d", len(b.get("market_id", [])))

    async def _insert_with_retry(self, table, rows, cols):
        """Insert a list of row dicts into ClickHouse with retries and batch-splitting on memory errors.

        This will NOT raise to the caller; on repeated failure the batch is dropped after logging.
        """
        max_attempts = 5
        delay = 1.0

        # Build row-oriented list suitable for clickhouse client (list of tuples)
        # Ensure ordering of columns matches `cols`
        row_tuples = [tuple(r.get(c) for c in cols) for r in rows]

        for attempt in range(1, max_attempts + 1):
            try:
                # prefer row-oriented insert with explicit column names
                self.client.insert(table, row_tuples, column_names=cols)
                # update sink metrics
                try:
                    self.inserts_total += len(rows)
                    self.last_insert_ts = datetime.now(timezone.utc)
                except Exception:
                    pass
                log.info("clickhouse_insert_ok", extra={"rows": len(rows), "attempt": attempt})
                return
            except (DatabaseError, OperationalError) as e:
                msg = str(e)
                category = self._classify_ch_error(msg)
                log.warning(
                    "clickhouse_insert_failed",
                    extra={
                        "rows": len(rows),
                        "attempt": attempt,
                        "category": category,
                        "error": msg[:500],
                    },
                )

                # If memory exceeded, try splitting batch in half (if possible)
                if "MEMORY_LIMIT_EXCEEDED" in msg and len(rows) > 1:
                    mid = len(rows) // 2
                    await self._insert_with_retry(table, rows[:mid], cols)
                    await self._insert_with_retry(table, rows[mid:], cols)
                    return

                if attempt == max_attempts:
                    # final give-up: increment failure metric and log
                    try:
                        self.insert_failures += 1
                    except Exception:
                        pass
                    log.error(
                        "clickhouse_insert_gave_up",
                        extra={"rows": len(rows), "category": category},
                    )
                    return

                await asyncio.sleep(delay)
                delay *= 2

    def _classify_ch_error(self, msg: str) -> str:
        if "MEMORY_LIMIT_EXCEEDED" in msg:
            return "memory_limit"
        if "Connection refused" in msg:
            return "connection_refused"
        if "NameResolutionError" in msg:
            return "dns_resolution"
        return "other"

