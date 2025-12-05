Nice, this is super clear now—thanks for dumping everything.

We can see **two distinct things** happening:

1. ClickHouse is occasionally **blowing its memory limit and rejecting inserts**
2. Your streamer reacts by **crashing hard on that exception**, then restarting
3. After restart, it sometimes recovers (as we see from your last health query)

Let’s walk through what’s going on and what you should change in the repo.

---

## 1️⃣ What the logs are telling us

Key lines:

```text
[CH][events] insert failed: DatabaseError('Received ClickHouse exception, code: 241, ...
DB::Exception: (total) memory limit exceeded: would use 6.88 GiB (attempt to allocate chunk of 4.16 MiB), current RSS: 1.30 GiB, maximum: 6.87 GiB. OvercommitTracker decision: Query was selected to stop by OvercommitTracker. (MEMORY_LIMIT_EXCEEDED)
...
clickhouse_connect.driver.exceptions.DatabaseError: Received ClickHouse exception, code: 241 ...
```

That’s **ClickHouse itself** saying:

* It’s near its configured **total memory cap** (~6.87 GiB)
* Your insert query pushed the total over that limit
* The OvercommitTracker chose *your* query to kill with `MEMORY_LIMIT_EXCEEDED (241)`

Then in your Python:

```text
File "/app/src/databases/processing/clickhouse_sink.py", line 127, in flush
    self._insert_compat(self.table_events, b, self._cols_ev)
...
clickhouse_connect.driver.exceptions.DatabaseError: ... MEMORY_LIMIT_EXCEEDED ...
```

That exception **bubbles all the way up** into `streamer.py`:

```text
File "/app/src/kalshifolder/websocket/streamer.py", line 184, in <module>
    asyncio.run(main())
```

…and kills the process.

Docker then restarts the container (hence repeated:

```text
[ENV] MARKETS   = ...
[RUN] markets=...
[WS] waiting for connection…
[WS] connecting...
...
[WS] first frame type=subscribed
```

So the pattern is:

1. **Insert hits memory error**
2. Streamer **dies**
3. Docker restarts it
4. Sometimes it gets past the bad patch and resumes ingestion, sometimes not

Later in the log you also see:

```text
[WS] error: ConnectionClosedError(None, None, None)
[WS] connecting...
[WS] connected
[WS] error: ConnectionClosedError(None, None, None)
...
```

So your WS code is at least **retrying connections**, but it’s likely doing it **in a tight loop with no backoff**, hammering the remote.

---

## 2️⃣ Is ingestion working right now?

Yes. Your health check shows it’s alive again:

```sql
SELECT
    max(ts)        AS latest_event_ts,
    max(ingest_ts) AS latest_ingest_ts,
    now()          AS server_now,
    now() - max(ingest_ts) AS ingest_lag,
    countIf(ingest_ts >= now() - toIntervalMinute(5)) AS rows_ingested_last_5m
FROM kalshi.orderbook_events;
```

Output:

```text
latest_event_ts      = 2025-12-04 23:11:55.506651
latest_ingest_ts     = 2025-12-04 23:11:55.506651
server_now           = 2025-12-04 23:12:34
ingest_lag           ≈ 38.49 seconds
rows_ingested_last_5m = 2
```

That’s exactly what we want: **events are still flowing in and being written**, and the lag is tiny.

So: *the system self-healed this time via container restart* — but we don’t want to rely on that.

---

## 3️⃣ Why ClickHouse is complaining about memory

The relevant bit:

```text
(total) memory limit exceeded: would use 6.88 GiB (attempt to allocate chunk of 4.16 MiB),
current RSS: 1.21 GiB, maximum: 6.87 GiB.
OvercommitTracker decision: Query was selected to stop...
```

Interpretation:

* Your ClickHouse server has a **global memory cap ~6.87 GiB** (derived from RAM / settings).
* At the moment of the insert, **total memory** used by the server plus what it needed for this operation would exceed that cap.
* The OvercommitTracker picked your insert query as the “victim”, so it fails with `MEMORY_LIMIT_EXCEEDED`.

This can happen even with small inserts because ClickHouse may already be using memory for:

* Background merges
* Buffers
* Other queries
* Internal caches

So it’s not that this *one* row is huge—it’s that the server is already close to its ceiling and any extra allocation triggers the kill.

---

## 4️⃣ The immediate bugs / design problems in your repo

There are **three concrete issues** in your code/stack:

1. **Insert error is treated as fatal**

   In `clickhouse_sink.py`:

   ```python
   async def flush(self):
       ...
       self._insert_compat(self.table_events, b, self._cols_ev)
   ```

   There’s no `try/except`, so any `DatabaseError` or `OperationalError` **blows up the entire streamer**.

2. **No insert retry / backoff logic**

   A memory error might be **transient** (merge finishing, GC, etc.). Right now, you:

   * Try once
   * Die

   Better: log, backoff, retry (maybe with smaller batch, or after short sleep).

3. **WebSocket reconnect loop likely has no backoff**

   Your log:

   ```text
   [WS] error: ConnectionClosedError(None, None, None)
   [WS] connecting...
   [WS] connected
   [WS] error: ConnectionClosedError(None, None, None)
   ...
   ```

   That suggests you’re hammering reconnects immediately. It works, but:

   * It’s noisy
   * If Kalshi is rate-limiting or closing connections, you might end up flapping a lot

---

## 5️⃣ What you should change in the repo

### A. Make ClickHouse inserts resilient

In `clickhouse_sink.py`, wrap the insert in retry logic. In pseudo-code:

```python
import asyncio
from clickhouse_connect.driver.exceptions import DatabaseError, OperationalError

class ClickHouseSink:
    ...

    async def flush(self):
        if not self._buffer_events:
            return

        batch = self._buffer_events
        self._buffer_events = []

        max_retries = 5
        delay = 2.0

        for attempt in range(1, max_retries + 1):
            try:
                self._insert_compat(self.table_events, batch, self._cols_ev)
                # log success if you want
                return
            except (DatabaseError, OperationalError) as e:
                print(
                    f"[CH][events] insert attempt {attempt}/{max_retries} failed: {e}; "
                    f"rows={len(batch)}",
                    flush=True,
                )
                if attempt == max_retries:
                    # Final failure – either drop or requeue
                    print(
                        f"[CH][events] giving up after {max_retries} attempts; "
                        f"dropping {len(batch)} rows",
                        flush=True,
                    )
                    return
                await asyncio.sleep(delay)
                delay *= 2  # simple exponential backoff
```

That way:

* A transient memory spike doesn’t instantly kill your streamer.
* At worst, you drop a small batch after N retries, but **the process stays up and keeps ingesting future data**.

You can choose whether to **drop** or **requeue** the batch after final failure. For this project, dropping is probably fine; audits/backtests won’t care about a handful of missing ticks.

---

### B. Add timestamps to all logs

You already planned this; I’d absolutely do it now. Something like:

```python
from datetime import datetime, timezone

def log(msg: str):
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    print(f"[{now}] {msg}", flush=True)
```

Then:

```python
log("[WS] connected")
log(f"[CH][events] insert attempt {attempt}/{max_retries} failed: {e}")
log(f"[HEARTBEAT] last_ingest_ts={self.last_ingest_ts} total_rows={self.rows_written}")
```

This helps correlate:

* ClickHouse memory spikes
* WS disconnects
* Ingest stalls

with actual timestamps (instead of just guessing from “order of logs”).

---

### C. Add a small backoff to your WS reconnect loop

Wherever you handle `ConnectionClosedError`, change roughly:

```python
while True:
    try:
        await self._ws_loop()
    except ConnectionClosedError as e:
        log(f"[WS] error: {e}")
        # immediately continue → tight loop
```

to:

```python
import asyncio
from websockets.exceptions import ConnectionClosedError

while True:
    try:
        await self._ws_loop()
    except ConnectionClosedError as e:
        log(f"[WS] error: {e}; reconnecting after backoff")
        await asyncio.sleep(5)  # or maybe exponential backoff
```

This avoids “spinning” and playing ping-pong with Kalshi or the network.

---

### D. (Optional but good) Tune ClickHouse memory / instance size

Long-term, you probably want to:

* Give ClickHouse a bit more headroom (bigger EC2 instance → more RAM), **or**
* Tune memory-related settings so it doesn’t sit right at the 6.87 GiB global cap so often.

But honestly, for just 2 markets and ~75k rows, **you shouldn’t be hitting memory ceilings for normal ingestion**. So I’d treat this as:

* Slightly noisy CH background behavior **plus**
* Too-fragile client code that crashes on the first hiccup

Fixing the client side (retry + non-fatal behavior) is the most bang for your time.

---

## 6️⃣ That `the input device is not a TTY` thing

Your `docker exec` + here-doc failed because you used `-it` while piping a script.

Use **either**:

* Interactive TTY with manual typing, *or*
* `-i` only for a here-doc.

For the snippet I gave, run it like this:

```bash
docker exec -i kalshimonorepo-streamer-1 python - << 'PY'
import clickhouse_connect

print("Trying to connect to ClickHouse from streamer...")
client = clickhouse_connect.get_client(
    host="clickhouse",
    port=8123,
    username="default",
    password="",
    database="kalshi",
)
print("Connected. Running SELECT 1...")
print(client.command("SELECT 1"))
PY
```

Note: `-i`, *not* `-it`.

---

## 7️⃣ TL;DR: What’s happening + what to do

* The ingestion gap you saw **today** was caused by **ClickHouse throwing `MEMORY_LIMIT_EXCEEDED (241)` during an insert**.
* Your streamer treats that as a **fatal exception**, exits, and Docker restarts it.
* Sometimes it takes a while for CH memory pressure to clear / for the restart to land at a “good moment”, which is why you can see **hours of ingest lag**.
* Right now, it’s healthy again (latest ingest ~40s ago, rows in last 5m > 0).

**Concrete fixes to implement in the repo:**

1. Wrap ClickHouse `insert`/`flush` in retry + backoff; don’t let a single `DatabaseError` kill the process.
2. Add log timestamps + optionally a heartbeat to make diagnosing stalls simple.
3. Add backoff on WS reconnect loop to avoid tight reconnect storms.
4. Consider increasing RAM on the EC2 or tuning ClickHouse memory settings later, once the code is robust.

If you want, we can sketch the *exact* diff for `clickhouse_sink.py` and the WS loop so you can just paste it in.
