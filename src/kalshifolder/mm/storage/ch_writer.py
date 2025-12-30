import requests
import time
import logging
from pathlib import Path

from ..utils.time import now_ms

logger = logging.getLogger(__name__)


class ClickHouseWriter:
    def __init__(self, url: str, user: str = 'default', pwd: str = '', database: str = 'default'):
        self.url = url.rstrip('/')
        self.user = user
        self.pwd = pwd
        self.database = database
        logger.info("CH init url=%s db=%s user=%s pwd_set=%s", self.url, self.database, self.user, bool(self.pwd))

    def _exec(self, sql: str, params: dict = None, timeout: int = 10):
        q = sql
        try:
            req_params = {"database": self.database}
            if self.user:
                req_params["user"] = self.user
            if self.pwd:
                req_params["password"] = self.pwd
            if params:
                req_params.update(params)

            r = requests.post(
                self.url,
                params=req_params,
                data=q.encode("utf-8"),
                timeout=timeout,
            )
            r.raise_for_status()
            return r.text
        except Exception:
            logger.exception("ClickHouse exec failed")
            raise

    def ensure_schema(self, sql_path: str):
        p = Path(sql_path)
        if not p.exists():
            logger.warning('schemas.sql not found: %s', sql_path)
            return
        sql = p.read_text()
        self._exec(sql)

    def insert(self, table: str, csv_rows: str):
        # Accept either a preformatted CSV string or a list/dict to convert to CSVWithNames
        if isinstance(csv_rows, (list, tuple)):
            # list of dicts
            rows = csv_rows
        else:
            # could be a single dict or already CSV text
            if isinstance(csv_rows, dict):
                rows = [csv_rows]
            elif isinstance(csv_rows, str):
                # assume caller provided CSVWithNames body already
                sql = f"INSERT INTO {table} FORMAT CSVWithNames\n{csv_rows}"
                return self._exec(sql)
            else:
                raise ValueError('csv_rows must be str, dict or list of dicts')

        # convert dicts to CSVWithNames
        import io, csv
        if not rows:
            return None
        fieldnames = list(rows[0].keys())
        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=fieldnames)
        writer.writeheader()
        for r in rows:
            writer.writerow({k: ('' if r.get(k) is None else r.get(k)) for k in fieldnames})
        csv_text = buf.getvalue()
        sql = f"INSERT INTO {table} FORMAT CSVWithNames\n{csv_text}"
        return self._exec(sql)
