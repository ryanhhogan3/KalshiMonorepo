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
        if r.status_code >= 300:
            logger.error("ClickHouse HTTP %s response: %s", r.status_code, (r.text or "")[:2000])
        r.raise_for_status()
        return r.text

    def ensure_schema(self, sql_path: str):
        p = Path(sql_path)
        if not p.exists():
            logger.warning("schemas.sql not found: %s", sql_path)
            return

        raw = p.read_text(encoding="utf-8").lstrip("\ufeff").replace("\r\n", "\n")
        statements = [s.strip() for s in raw.split(";") if s.strip()]

        for stmt in statements:
            # Remove leading comment lines, but keep the statement
            lines = []
            for line in stmt.splitlines():
                if not lines and line.strip().startswith("--"):
                    continue
                lines.append(line)
            cleaned = "\n".join(lines).strip()
            if not cleaned:
                continue
            self._exec(cleaned)
        
        # Apply schema migrations (column additions, etc.)
        self._apply_schema_migrations()

    def _apply_schema_migrations(self):
        """Apply column additions and other migrations that CREATE TABLE IF NOT EXISTS doesn't handle."""
        try:
            # Ensure mm_fills has required columns
            self._ensure_column('mm_fills', 'fill_id', 'String')
            self._ensure_column('mm_fills', 'action', 'String')
            
            # Log current schema for debugging
            result = self._exec_and_read(f"DESCRIBE TABLE {self.database}.mm_fills")
            if result:
                columns = [row.split('\t')[0] for row in result.strip().split('\n')]
                logger.info(f"mm_fills columns: {columns}")
        except Exception as e:
            logger.warning(f"Schema migration warning (non-blocking): {e}")

    def _ensure_column(self, table: str, column: str, column_type: str):
        """Add a column to a table if it doesn't already exist."""
        try:
            # Check if column exists
            result = self._exec_and_read(f"SELECT 1 FROM {self.database}.{table} WHERE 1=0 LIMIT 0")
            
            # Try to read the column - if it fails, column doesn't exist
            try:
                self._exec(f"SELECT {column} FROM {self.database}.{table} LIMIT 0")
                # Column exists, nothing to do
                logger.debug(f"{table}.{column} already exists")
                return
            except:
                # Column doesn't exist, add it
                alter_stmt = f"ALTER TABLE {self.database}.{table} ADD COLUMN {column} {column_type}"
                logger.info(f"Running migration: {alter_stmt}")
                self._exec(alter_stmt)
                logger.info(f"Successfully added {table}.{column} ({column_type})")
        except Exception as e:
            logger.warning(f"Failed to ensure {table}.{column}: {e}")

    def _exec_and_read(self, sql: str, timeout: int = 10) -> str:
        """Execute SQL and return result as string."""
        q = sql
        req_params = {"database": self.database}
        if self.user:
            req_params["user"] = self.user
        if self.pwd:
            req_params["password"] = self.pwd

        try:
            resp = requests.get(self.url, params=req_params, data=q.encode("utf-8"), timeout=timeout)
            resp.raise_for_status()
            return resp.text
        except requests.RequestException as e:
            logger.error(f"CH read error: {e}")
            raise


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
