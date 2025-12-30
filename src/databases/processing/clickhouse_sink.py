# src/databases/processing/clickhouse_sink.py
import asyncio, os, json
from datetime import datetime, timezone
from dataclasses import dataclass, field
from collections import deque
import urllib.parse as up
import clickhouse_connect
from clickhouse_connect.driver.exceptions import DatabaseError, OperationalError
import logging

from .clickhouse_bootstrap import ensure_clickhouse_schema
from .clickhouse_health import assert_clickhouse_ready

log = logging.getLogger(__name__)


@dataclass
class InsertStats:
    insert_failures_total: int = 0
    # timestamps (float seconds) of recent failures for windowed count
    _recent_fail_ts: deque = field(default_factory=lambda: deque())
    last_insert_error: str | None = None
    last_insert_error_ts: float | None = None
    last_success_insert_ts: float | None = None
    inserts_total: int = 0
    pending_rows: int = 0

    def record_failure(self, error_msg: str, now_ts: float, window_s: int = 60):
        self.insert_failures_total += 1
        self.last_insert_error = error_msg[:500]
        self.last_insert_error_ts = now_ts
        self._recent_fail_ts.append(now_ts)
        # purge old
        cutoff = now_ts - window_s
        while self._recent_fail_ts and self._recent_fail_ts[0] < cutoff:
            self._recent_fail_ts.popleft()

    def recent_fail_count(self) -> int:
        return len(self._recent_fail_ts)

    def record_success(self, now_ts: float, rows: int = 0):
        self.last_success_insert_ts = now_ts
        if rows:
            self.inserts_total += rows


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
            tables=("orderbook_events", "latest_levels_v2"),
            max_broken_parts=max_broken,
        )

        self.table_events = "orderbook_events"
        self.table_latest = "latest_levels_v2"
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
        # Insert stats object (used by streamer heartbeat)
        self.insert_stats = InsertStats()

        # Backwards compat aliases
        self.insert_failures = 0
        self.inserts_total = 0
        self.last_insert_ts = None

        # Spool config
        self.spool_dir = os.getenv("CLICKHOUSE_SPOOL_DIR", "/app/spool")
        self.spool_max_bytes = int(os.getenv("CLICKHOUSE_SPOOL_MAX_BYTES", str(100 * 1024 * 1024)))
        os.makedirs(self.spool_dir, exist_ok=True)

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
            # record pending rows
            self.insert_stats.pending_rows = len(batch)
            await self._insert_with_retry(self.table_events, batch, self._cols_ev)
            self.insert_stats.pending_rows = 0

        # latest_levels buffer (columnar)
        if self._buf_lt["market_id"]:
            b = self._buf_lt
            self._buf_lt = self._new_lt()
            # convert columnar buffer to rows
            rows = []
            n = len(b.get("market_id", []))
            for i in range(n):
                row = {c: b[c][i] for c in self._cols_lt}
                rows.append(row)
            # record pending rows
            self.insert_stats.pending_rows = len(rows)
            await self._insert_with_retry(self.table_latest, rows, self._cols_lt)
            self.insert_stats.pending_rows = 0

    async def _insert_with_retry(self, table, rows, cols):
        """Insert a list of row dicts into ClickHouse with retries and spool-on-giveup.

        This will NOT raise to the caller; on repeated failure the batch is spooled to disk.
        """
        max_attempts = int(os.getenv("CLICKHOUSE_MAX_INSERT_ATTEMPTS", "5"))
        delay = float(os.getenv("CLICKHOUSE_INSERT_BACKOFF_SECS", "1.0"))
        jitter = float(os.getenv("CLICKHOUSE_INSERT_JITTER", "0.25"))
        fail_window = int(os.getenv("CLICKHOUSE_INSERT_FAIL_WINDOW_S", "60"))

        # Build row-oriented list suitable for clickhouse client (list of tuples)
        # Ensure ordering of columns matches `cols`
        def _sanitize_val(col, val):
            # Never insert empty-string for UUID market_id; prefer None
            if col == "market_id" and val == "":
                return None
            return val

        row_tuples = [tuple(_sanitize_val(c, r.get(c)) for c in cols) for r in rows]

        for attempt in range(1, max_attempts + 1):
            try:
                # prefer row-oriented insert with explicit column names
                self.client.insert(table, row_tuples, column_names=cols)
                # update sink metrics and insert_stats
                try:
                    now_ts = datetime.now(timezone.utc)
                    self.inserts_total += len(rows)
                    self.last_insert_ts = now_ts
                    self.insert_stats.record_success(now_ts.timestamp(), rows=len(rows))
                    # keep aliases in sync
                    self.insert_failures = self.insert_stats.insert_failures_total
                    self.inserts_total = self.insert_stats.inserts_total
                except Exception:
                    pass
                log.info(
                    "clickhouse_insert_ok",
                    extra={"rows": len(rows), "attempt": attempt, "table": table},
                )
                return
            except (DatabaseError, OperationalError, Exception) as e:
                now = datetime.now(timezone.utc)
                now_ts = now.timestamp()
                msg = f"{e.__class__.__name__}: {str(e)}"
                category = self._classify_ch_error(str(e))

                # record failure in stats
                try:
                    self.insert_stats.record_failure(msg, now_ts, window_s=fail_window)
                    # keep alias in sync
                    self.insert_failures = self.insert_stats.insert_failures_total
                except Exception:
                    pass

                # one structured log line per failure attempt
                log.warning(
                    "clickhouse_insert_failed",
                    extra={
                        "rows": len(rows),
                        "attempt": attempt,
                        "attempts_max": max_attempts,
                        "table": table,
                        "category": category,
                        "error": msg[:500],
                    },
                )

                # If memory exceeded, try splitting batch in half (if possible)
                if "MEMORY_LIMIT_EXCEEDED" in str(e) and len(rows) > 1:
                    mid = len(rows) // 2
                    await self._insert_with_retry(table, rows[:mid], cols)
                    await self._insert_with_retry(table, rows[mid:], cols)
                    return

                if attempt == max_attempts:
                    # final give-up: spool to disk for later replay
                    try:
                        spooled_file = self._spool_rows(rows, cols)
                        log.error(
                            "clickhouse_insert_gave_up",
                            extra={
                                "rows": len(rows),
                                "table": table,
                                "category": category,
                                "error": msg[:500],
                                "spooled_file": spooled_file,
                            },
                        )
                        # also ensure insert_stats totals reflect gave-up
                        self.insert_stats.record_failure(msg, now_ts, window_s=fail_window)
                        self.insert_failures = self.insert_stats.insert_failures_total
                    except Exception:
                        log.exception("Failed to spool rows after give-up")
                    return

                # exponential backoff + jitter
                await asyncio.sleep(delay + (jitter * (os.urandom(1)[0] / 255.0)))
                delay *= 2

    def _classify_ch_error(self, msg: str) -> str:
        if "MEMORY_LIMIT_EXCEEDED" in msg:
            return "memory_limit"
        if "Connection refused" in msg:
            return "connection_refused"
        if "NameResolutionError" in msg:
            return "dns_resolution"
        return "other"

    def _spool_rows(self, rows: list[dict], cols: list[str]) -> str:
        """Append rows as NDJSON to a spool file and rotate when exceeding size limit.

        Returns the path of the spool file written to.
        """
        # write to current spool file
        base = os.path.join(self.spool_dir, "spool_current.ndjson")
        try:
            with open(base, "a", encoding="utf8") as fh:
                for r in rows:
                    # include columns to make replay easier
                    payload = {c: r.get(c) for c in cols}
                    fh.write(json.dumps(payload, default=str))
                    fh.write("\n")

            # rotate if too large
            try:
                sz = os.path.getsize(base)
                if sz > self.spool_max_bytes:
                    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
                    rotated = os.path.join(self.spool_dir, f"spool_{ts}.ndjson")
                    os.replace(base, rotated)
                    return rotated
            except Exception:
                pass
            return base
        except Exception:
            log.exception("Failed to write spool file")
            raise

