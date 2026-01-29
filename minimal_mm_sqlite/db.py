from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from typing import Any, Dict, Optional


SCHEMA_SQL = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;

CREATE TABLE IF NOT EXISTS decisions (
  id TEXT PRIMARY KEY,
  ts_ms INTEGER NOT NULL,
  market TEXT NOT NULL,
  yes_bb_cents INTEGER,
  yes_ba_cents INTEGER,
  md_age_ms INTEGER,
  allowed INTEGER NOT NULL,
  reason TEXT NOT NULL,
  target_yes_bid_cents INTEGER,
  target_no_bid_cents INTEGER,
  size INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS actions (
  id TEXT PRIMARY KEY,
  decision_id TEXT NOT NULL,
  ts_ms INTEGER NOT NULL,
  market TEXT NOT NULL,
  action_type TEXT NOT NULL,  -- PLACE|CANCEL
  side TEXT NOT NULL,         -- BID|ASK
  api_side TEXT NOT NULL,     -- yes|no
  client_order_id TEXT NOT NULL,
  price_cents INTEGER,
  size INTEGER,
  request_json TEXT,
  FOREIGN KEY(decision_id) REFERENCES decisions(id)
);

CREATE TABLE IF NOT EXISTS responses (
  action_id TEXT PRIMARY KEY,
  ts_ms INTEGER NOT NULL,
  market TEXT NOT NULL,
  client_order_id TEXT NOT NULL,
  status TEXT NOT NULL,
  exchange_order_id TEXT,
  reject_reason TEXT,
  latency_ms INTEGER,
  response_json TEXT,
  FOREIGN KEY(action_id) REFERENCES actions(id)
);

CREATE TABLE IF NOT EXISTS positions (
  ts_ms INTEGER NOT NULL,
  market TEXT NOT NULL,
  pos REAL NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_positions_market_ts ON positions(market, ts_ms);
"""


@dataclass
class SQLiteDB:
    path: str

    def connect(self) -> sqlite3.Connection:
        con = sqlite3.connect(self.path)
        con.row_factory = sqlite3.Row
        return con

    def ensure_schema(self) -> None:
        con = self.connect()
        try:
            con.executescript(SCHEMA_SQL)
            con.commit()
        finally:
            con.close()


class SQLiteWriter:
    def __init__(self, db: SQLiteDB):
        self._db = db

    def insert_decision(self, row: Dict[str, Any]) -> None:
        self._insert("decisions", row)

    def insert_action(self, row: Dict[str, Any]) -> None:
        self._insert("actions", row)

    def insert_response(self, row: Dict[str, Any]) -> None:
        self._insert("responses", row)

    def insert_position(self, *, ts_ms: int, market: str, pos: float) -> None:
        self._insert("positions", {"ts_ms": ts_ms, "market": market, "pos": float(pos)})

    def _insert(self, table: str, row: Dict[str, Any]) -> None:
        con = self._db.connect()
        try:
            cols = list(row.keys())
            vals = [row[c] for c in cols]
            placeholders = ",".join(["?"] * len(cols))
            sql = f"INSERT OR REPLACE INTO {table} ({','.join(cols)}) VALUES ({placeholders})"
            con.execute(sql, vals)
            con.commit()
        finally:
            con.close()


def dumps(obj: Any) -> str:
    return json.dumps(obj, separators=(",", ":"), ensure_ascii=False)


def opt_int(v: Optional[Any]) -> Optional[int]:
    try:
        return None if v is None else int(v)
    except Exception:
        return None
