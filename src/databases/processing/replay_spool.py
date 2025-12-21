import os
import json
import asyncio
import logging
from datetime import datetime, timezone

from .clickhouse_sink import ClickHouseSink

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

SPOOL_DIR = os.getenv("CLICKHOUSE_SPOOL_DIR", "/app/spool")

async def replay_file(sink: ClickHouseSink, path: str):
    log.info("Replaying spool file: %s", path)
    rows = []
    with open(path, "r", encoding="utf8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                rows.append(obj)
            except Exception as e:
                log.exception("Failed to parse line in %s: %s", path, e)

    if not rows:
        log.info("No rows to replay for %s", path)
        return True

    try:
        await sink._insert_with_retry(sink.table_events, rows, sink._cols_ev)
        # on success, rotate file
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        dest = path + ".replayed." + ts
        os.replace(path, dest)
        log.info("Replayed and archived %s -> %s", path, dest)
        return True
    except Exception:
        log.exception("Failed to replay %s", path)
        return False


def main():
    sink = ClickHouseSink()
    files = []
    if os.path.isdir(SPOOL_DIR):
        for name in os.listdir(SPOOL_DIR):
            if name.endswith('.ndjson'):
                files.append(os.path.join(SPOOL_DIR, name))
    files.sort()
    if not files:
        log.info("No spool files to replay in %s", SPOOL_DIR)
        return
    for f in files:
        asyncio.run(replay_file(sink, f))


if __name__ == '__main__':
    main()
