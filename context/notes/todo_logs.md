1. Make logs operational, not just descriptive

Right now you log “what happened.” Add things that answer “is the system healthy?” at a glance:

A. Periodic health / heartbeat logs

In the streamer loop, every N seconds/minutes log a structured heartbeat:

Last ingest_ts

Last ts from Kalshi

Ingest lag (seconds)

Rows flushed since last heartbeat

Buffer size

Example:

log.info(
    "heartbeat",
    extra={
        "component": "streamer",
        "last_event_ts": str(last_event_ts),
        "last_ingest_ts": str(last_ingest_ts),
        "ingest_lag_sec": ingest_lag,
        "rows_buffered": len(self._buffer_events),
        "rows_written_total": self.rows_written,
    },
)


This gives you a clean “timeline” of lag/data-flow without having to run ClickHouse queries every time.

2. Add structured logging for key events

You already have a good framework; next step is to stop relying on free-form message text for important events.

For critical paths (WS events, inserts, retries), log with a consistent set of fields:

component: "ws" | "clickhouse" | "streamer"

event: "ws_connected" | "ws_error" | "insert_failed" | "insert_retry"

market_tickers: list or CSV

rows: int (for inserts)

attempt: retry attempt

exception_type, exception_msg

Even if you don’t yet pipe this into a log aggregator, future you or anyone reading logs can grep on event= or component=.

3. Separate levels & channels clearly

Define and document what each level means in your repo:

DEBUG – extremely noisy: every frame, every delta, buffer size changes

INFO – lifecycle and high-level ops:

process start/stop

WS (connect / disconnect / resubscribe)

flush success (but maybe rate-limited, e.g., “every 1k rows”)

heartbeats

WARNING – transient problems that you recover from:

single failed insert (with retry)

transient WS errors that were auto-recovered

ERROR – things you couldn’t recover from but process still alive:

dropped batches after max retries

CRITICAL – process is going to die / fatal configuration problem

Then wire:

Console handler → INFO and above

File handler → DEBUG and above

This way normal docker logs stays readable, and deep dives go to file.

4. Log ClickHouse error categories explicitly

You’ve already hit:

MEMORY_LIMIT_EXCEEDED (241)

Connection errors (refused / name resolution)

In your insert wrapper, parse and categorize:

from clickhouse_connect.driver.exceptions import DatabaseError, OperationalError

def classify_ch_error(e: Exception) -> str:
    msg = str(e)
    if "MEMORY_LIMIT_EXCEEDED" in msg or "memory limit exceeded" in msg:
        return "memory_limit"
    if "Connection refused" in msg:
        return "connection_refused"
    if "NameResolutionError" in msg:
        return "dns_resolution"
    return "other"


Then:

category = classify_ch_error(e)
log.warning(
    "clickhouse_insert_failed",
    extra={
        "event": "insert_failed",
        "category": category,
        "rows": len(batch),
        "attempt": attempt,
    },
)


This makes it super obvious later if:

You’re mostly hitting memory issues, or

It’s actually network / DNS / config.

5. Add startup + config dumps

On startup, log a one-shot config snapshot (redacting secrets):

EC2 instance type (if you pass it in / env)

Markets

ClickHouse URL / db / user (no password)

Flush thresholds (buffer size, time)

WS URL

Logging level / log file path

Example:

log.info(
    "startup_config",
    extra={
        "markets": markets,
        "ch_url": ch_url,
        "ch_database": ch_db,
        "flush_rows_threshold": self.flush_rows,
        "flush_secs_threshold": self.flush_secs,
        "log_level": logging.getLevelName(logger.level),
    },
)


When something is weird months from now, you can confirm exactly what config the process launched with.

6. Add graceful shutdown logging

Trap SIGTERM / SIGINT in the streamer, so Docker stops look explicit:

import signal

def setup_signal_handlers(loop, stop_event):
    def handler(sig):
        log.info(f"received_signal", extra={"signal": str(sig)})
        stop_event.set()
    for s in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(s, handler, s)


Then in main():

Log: "[LIFECYCLE] shutdown requested"

Wait for in-flight insert flush

Log: "[LIFECYCLE] shutdown complete"

That way you can distinguish:

“Process crashed with stack trace” vs

“Process shut down cleanly (deploy, instance stop, etc.)”

7. Make a tiny “How to read logs” section in LOGGING_GUIDE.md

You already have a logging guide—add a short practical section like:

Common Patterns to Look For

Ingestion stall

Symptom: heartbeat logs show ingest_lag_sec growing, rows_ingested_last_5m = 0 in ClickHouse.

Likely causes:

Repeated clickhouse_insert_failed with category memory_limit

Repeated [WS] error: ConnectionClosedError with reconnect cycles.

WS flapping

Symptom: Many ws_error → ws_connecting → ws_connected messages per minute.

Action: Check network, Kalshi rate limits, and backoff settings.

This turns your logs into an actual debugging playbook.

8. Integrate with host / cloud logs (optional but strong)

When you get tired of SSHing into the box:

Pipe Docker logs to CloudWatch, Loki, or whatever you like.

Use your structured fields (component, event, category, etc.) as filters.

You don’t have to do this right away, but your current logging structure is already pretty ready for it.

9. Guard against logging volume explosions

Last thing: because you’re dealing with high-frequency data:

Make sure you don’t log per-event for normal ticks at INFO.

Keep per-event logging at DEBUG and off by default in production.

Use sampling if you ever add noisy logs:

import random

if random.random() < 0.01:
    log.debug("sampled_event", extra={...})


So logs stay useful and small-ish instead of turning into a firehose.