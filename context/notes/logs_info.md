✅ 1. Docker Logs (stdout/stderr)

This is the one you’re already looking at:

docker logs -f kalshi_streamer


This shows:

✔ High-level logs:

Session start/end

Environment dump (MARKETS, CH_URL, etc.)

WebSocket lifecycle logs

Subscription confirmations

Retry attempts

Heartbeat messages

Stall warnings

ClickHouse insert warnings/errors

Fatal exceptions if the process ever exits

✔ Exactly what you're seeing right now:
INFO | streamer_heartbeat ...
INFO | ws_error ...
WARNING | no_insert_activity ...
ERROR | Fatal error ...


This is the public, real-time operational output.

✅ 2. Rotating File Logs (inside the container)

These come from logging_config.py and the session_logger.
They rotate automatically so logs don't grow forever.

Inside the running container:

/app/logs/


Contains:

A. Session Logs (one file per run)

Example filename:

SESSION_20251206_201819.log


These include:

Full stack traces

Structured details about errors

Heartbeat summaries

All ClickHouse insert retries/splits

WS reconnect stack traces

Summary on shutdown

These session logs are the deep diagnostic logs — better than Docker logs.

B. Rotating Module Logs

Log file:

kalshifolder_websocket_streamer_logged.log


With rotation:

max-size: 10MB

max-files: 10

These contain module-level logs from:

ws_runtime

clickhouse_sink

websocket consumer

streamer flow

everything logged via the normal module logger

These are ongoing logs, not per-session.

✅ 3. ClickHouse Server Logs (inside the clickhouse container)

These are your database engine logs.

Inspectable with:

docker exec -it clickhouse bash
ls /var/log/clickhouse-server


You will see:

Query logs
clickhouse-server.log

Error logs
clickhouse-server.err.log


These show:

Background merge failures

Memory limit errors

Query timeouts

Corrupted parts (if ever again)

Table-level problems

Insert failures from clickhouse side

These logs are crucial during chaos testing.

You can tail them with:

docker exec -it clickhouse tail -f /var/log/clickhouse-server/clickhouse-server.log

🎯 Summary of All Logs Your System Now Captures
Log Source	What it Contains	Where to Access
Docker stdout/stderr	High-level runtime logs, heartbeats, warnings, fatal errors	docker logs -f kalshi_streamer
Session Logs	Detailed per-run trace, errors, inner retry logic	/app/logs/SESSION_*.log
Rotating Module Logs	Continuous low-level logs about WS/CH events	/app/logs/kalshifolder_websocket_*.log
ClickHouse Server Logs	DB engine errors, merges, memory, parts corruption	/var/log/clickhouse-server/*.log (inside clickhouse container)
🔥 You Now Have Full Observability

Your system is logging at all layers:

✔ WebSocket layer
✔ Streamer logic
✔ Insert pipelines
✔ Error + retry mechanisms
✔ Heartbeats for liveness checking
✔ Structured per-session logs
✔ ClickHouse engine logs
✔ Docker container lifecycle

This is equivalent to production-grade observability for a real streaming pipeline.