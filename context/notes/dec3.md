📄 Incident Report: ClickHouse Connectivity Failure in KalshiMonorepo Streamer
1. Summary of the Issue

Between Dec 3rd and Dec 4th, the KalshiMonorepo streamer container stopped ingesting data, resulting in no new rows being written to the orderbook_events table for ~20 hours.

Queries inside ClickHouse confirmed:

Last event timestamp froze at ~2025-12-03 23:16 UTC

No rows ingested in the last 5 minutes earlier in debugging

ingest_lag grew very large (hours)

However, the WebSocket portion of the streamer was still running successfully and logging subscription events.
The failure occurred before WebSocket processing: during ClickHouse client initialization.

2. Root Cause
We think, but are unsure 100% that the Cause: DNS Failure When Resolving the Internal Host clickhouse

The streamer logs repeatedly showed:

NameResolutionError: Failed to resolve 'clickhouse' ([Errno -3] Temporary failure in name resolution)


Meaning:

Docker tried to resolve the hostname clickhouse

The Docker network was not yet fully created / stabilized

The streamer container started too quickly

It attempted to connect to ClickHouse before service DNS was available

The connection failed → client init crashed → container restarted in a loop

WebSocket reconnection logic continued running, but ClickHouse inserts never executed

This is a docker-internal race condition, not an issue with your code.

3. How the Issue Was Solved
✔️ Solution Steps Taken

Stopped both containers

docker compose down


Rebuilt and recreated the network

docker compose up -d --build


After the rebuild:

Docker created a new bridge network

DNS registered clickhouse inside the network

Streamer successfully connected and re-initialized ClickHouse client

Validation queries showed the system was live again:

SELECT max(ts), max(ingest_ts)
FROM kalshi.orderbook_events;


→ timestamps matched now()
→ 71 rows ingested in last 5 minutes

Streamer was confirmed to be healthy.

4. Likely Contributing Factors
A. Slow EC2 instance startup / restart

Your EC2 instance restarted or network reloaded, causing:

Docker networks to reinitialize

Containers to come up asynchronously

ClickHouse to start slower than the streamer

B. Streamer container lacks retry logic for DB init

Currently the streamer:

Tries to connect once during startup

Immediately crashes if ClickHouse is unavailable

C. No explicit depends_on healthcheck wait

You have:

depends_on:
  clickhouse:
    condition: service_healthy


…but ClickHouse health check only uses curl http://localhost:8123/ping and may return OK before:

Tables exist

Server is fully ready to accept clients

D. Missing timestamps in stdout logs

This made correlating failure times harder.

5. Recommended Adjustments (Future Mitigation)
1. Add Retry Logic in ClickHouseSink Initialization

Instead of failing immediately, wrap startup in a retry loop:

for i in range(30):
    try:
        self.client = clickhouse_connect.get_client(...)
        break
    except Exception as e:
        print(f"[ClickHouse INIT RETRY] Attempt {i+1}/30 failed: {e}")
        time.sleep(2)
else:
    raise RuntimeError("Failed to initialize ClickHouse after 30 attempts")


Fixes all race-condition startup failures.

2. Add Timestamps to Streamer Logs (High Priority)

Wrap all prints:

def log(msg):
    print(f"[{datetime.now().isoformat()}] {msg}", flush=True)


Then replace all print(...) with log(...).

This will give you:

[2025-12-04T03:12:13.306325] [WS] connected
[2025-12-04T03:12:13.307450] [CH] successfully wrote batch of 2000 rows


Essential for diagnosing future outages.

3. Add a Timeout to ClickHouse Client Commands

For example:

self.client.command("SELECT 1", timeout=5)


Prevents long hangs that disguise underlying failures.

4. Strengthen the Docker Compose Healthcheck

Instead of simple curl, use:

healthcheck:
  test: ["CMD", "clickhouse-client", "-q", "SELECT 1"]
  interval: 5s
  timeout: 3s
  retries: 10


This ensures the DB is actually ready.

5. Consider Running Streamer as a Supervisor-Managed Service

E.g. a tiny supervisor container that restarts the streamer after DB is healthy.

(You don’t need this yet, but it’s a future scaling idea.)

6. Monitor Last Ingest Time

Write a helper script or dashboard that alerts if:

now() - max(ingest_ts) > 60 seconds


This detects failures immediately.

6. Final Status

Issue resolved

Streamer is back online

ClickHouse ingestion healthy

Future mitigations identified and documented

Here is the key query proving health:

SELECT
    max(ts) AS latest_event_ts,
    max(ingest_ts) AS latest_ingest_ts,
    now() AS server_now,
    now() - max(ingest_ts) AS ingest_lag,
    countIf(ingest_ts >= now() - toIntervalMinute(5)) AS rows_ingested_last_5m
FROM kalshi.orderbook_events;