import os, asyncio
from datetime import datetime, timezone
import pandas as pd
import pyarrow as pa, pyarrow.parquet as pq

class ParquetSink:
    def __init__(self, base_dir: str, *, rotate_secs=300, max_rows=250_000, max_bytes=100*1024*1024, compression="zstd"):
        self.base_dir = base_dir
        self.rotate_secs = rotate_secs
        self.max_rows = max_rows
        self.max_bytes = max_bytes  # New: max file size in bytes
        self.compression = compression
        self._buf = []
        self._t = asyncio.get_event_loop().time()

    def _dir_for(self, dt):
        d = dt.strftime("%Y-%m-%d")
        path = os.path.join(self.base_dir, "orderbook", f"date={d}")
        os.makedirs(path, exist_ok=True)
        return path

    def _estimate_buf_size(self) -> int:
        if not self._buf:
            return 0
        df = pd.DataFrame(self._buf)
        table = pa.Table.from_pandas(df, preserve_index=False)
        sink = pa.BufferOutputStream()
        pq.write_table(table, sink, compression=self.compression)
        return sink.size()

    async def add(self, row: dict):
        self._buf.append(row)
        now = asyncio.get_event_loop().time()
        buf_size = self._estimate_buf_size()
        if len(self._buf) >= self.max_rows or buf_size >= self.max_bytes or (now - self._t) >= self.rotate_secs:
            await self.flush()

    async def flush(self):
        if not self._buf: return
        df = pd.DataFrame(self._buf); self._buf = []; self._t = asyncio.get_event_loop().time()
        dt = datetime.now(timezone.utc)
        path = self._dir_for(dt)
        fn = f"part-{int(dt.timestamp()*1_000_000)}.parquet"
        table = pa.Table.from_pandas(df, preserve_index=False)
        pq.write_table(table, os.path.join(path, fn), compression=self.compression)
