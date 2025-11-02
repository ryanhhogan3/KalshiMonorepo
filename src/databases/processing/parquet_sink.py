import os, asyncio
from datetime import datetime, timezone
import pandas as pd
import pyarrow as pa, pyarrow.parquet as pq

class ParquetSink:
    def __init__(self, base_dir: str, *, rotate_secs=300, max_rows=250_000, compression="zstd"):
        self.base_dir = base_dir
        self.rotate_secs = rotate_secs
        self.max_rows = max_rows
        self.compression = compression
        self._buf = []
        self._t = asyncio.get_event_loop().time()

    def _dir_for(self, dt):
        d = dt.strftime("%Y-%m-%d")
        path = os.path.join(self.base_dir, "orderbook", f"date={d}")
        os.makedirs(path, exist_ok=True)
        return path

    async def add(self, row: dict):
        self._buf.append(row)
        now = asyncio.get_event_loop().time()
        if len(self._buf) >= self.max_rows or (now - self._t) >= self.rotate_secs:
            await self.flush()

    async def flush(self):
        if not self._buf: return
        df = pd.DataFrame(self._buf); self._buf = []; self._t = asyncio.get_event_loop().time()
        dt = datetime.now(timezone.utc)
        path = self._dir_for(dt)
        fn = f"part-{int(dt.timestamp()*1_000_000)}.parquet"
        table = pa.Table.from_pandas(df, preserve_index=False)
        pq.write_table(table, os.path.join(path, fn), compression=self.compression)
