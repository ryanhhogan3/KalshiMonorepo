# parquet_sink.py

import os, time, pyarrow as pa, pyarrow.parquet as pq
from datetime import datetime, timezone
import asyncio

class ParquetSink:
    def __init__(self, root_dir: str, *, rotate_secs=300.0, max_rows=250_000,
                 max_bytes=100*1024*1024,  # ~100MB target file size
                 size_check_every=1000,    # avoid computing size on every add()
                 compression_ratio=0.35):  # rough ZSTD/LZ4 ratio vs nbytes
        self.root = root_dir
        self.rotate_secs = float(rotate_secs)
        self.max_rows = int(max_rows)
        self.max_bytes = int(max_bytes)
        self.size_check_every = int(size_check_every)
        self.compression_ratio = float(compression_ratio)

        os.makedirs(self.root, exist_ok=True)
        self._buf = []              # list of dict rows
        self._last_rotate = time.time()
        self._lock = asyncio.Lock()
        self._adds_since_sizecheck = 0
        self._last_est_bytes = 0

    def _approx_buf_bytes(self) -> int:
        """Estimate on-disk bytes by converting to Arrow and scaling by compression ratio."""
        if not self._buf:
            return 0
        # Build arrow table in memory (no disk IO)
        table = pa.Table.from_pylist(self._buf)
        # Uncompressed memory footprint
        mem_bytes = table.nbytes
        # Heuristic to approximate compressed parquet size
        est_bytes = int(mem_bytes * self.compression_ratio)
        return est_bytes

    async def add(self, row: dict):
        async with self._lock:
            self._buf.append(row)
            self._adds_since_sizecheck += 1

            now = time.time()
            should_check_size = (self._adds_since_sizecheck >= self.size_check_every)
            if should_check_size:
                self._last_est_bytes = self._approx_buf_bytes()
                self._adds_since_sizecheck = 0

            need_rotate = (
                (self.max_rows and len(self._buf) >= self.max_rows)
                or (self.rotate_secs and (now - self._last_rotate) >= self.rotate_secs)
                or (self.max_bytes and self._last_est_bytes >= self.max_bytes)
            )
            if need_rotate:
                await self.flush()

    async def flush(self):
        if not self._buf:
            return
        # Snapshot and clear buffer quickly under lock
        buf = self._buf
        self._buf = []
        self._adds_since_sizecheck = 0
        self._last_est_bytes = 0
        self._last_rotate = time.time()

        # Write outside of lock to minimize contention
        table = pa.Table.from_pylist(buf)
        date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        dirp = os.path.join(self.root, "orderbook", f"date={date}")
        os.makedirs(dirp, exist_ok=True)
        ts_part = datetime.now(timezone.utc).strftime("%H%M%S_%f")
        path = os.path.join(dirp, f"part-{ts_part}.parquet")

        pq.write_table(
            table, path,
            compression="zstd",  # or "lz4"
            use_dictionary=True
        )
