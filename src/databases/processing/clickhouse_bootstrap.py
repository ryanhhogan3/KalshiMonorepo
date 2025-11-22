"""
clickhouse_bootstrap.py

Idempotent ClickHouse schema bootstrap and light schema migration utilities
for the KalshiMonorepo stack.

The goal of this module is:
- Create the `kalshi` database if it doesn't exist
- Ensure the core tables (`orderbook_events`, `latest_levels`) exist
- Ensure critical columns (like `size` in `latest_levels`) exist
- Be safe to call on every startup (idempotent)
"""

from __future__ import annotations

import logging
from typing import Optional, Iterable

from clickhouse_connect.driver.client import Client
from clickhouse_connect.driver.exceptions import DatabaseError, OperationalError, ProgrammingError

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Core DDL definitions
# ---------------------------------------------------------------------------

ORDERBOOK_EVENTS_DDL = """
CREATE TABLE IF NOT EXISTS {db}.orderbook_events
(
    `type` Enum8('snapshot' = 1, 'delta' = 2) CODEC(LZ4),
    `sid` UInt32 CODEC(Delta(4), ZSTD(3)),
    `seq` UInt64 CODEC(Delta(8), ZSTD(3)),
    `market_id` UUID CODEC(ZSTD(5)),
    `market_ticker` LowCardinality(String) CODEC(ZSTD(5)),
    `side` Enum8('yes' = 1, 'no' = 2) CODEC(LZ4),
    `price` UInt16 CODEC(DoubleDelta, ZSTD(3)),
    `qty` Int32 CODEC(Delta(4), ZSTD(3)),
    `ts` DateTime64(6, 'UTC') CODEC(DoubleDelta, ZSTD(3)),
    `ingest_ts` DateTime64(6, 'UTC') CODEC(DoubleDelta, ZSTD(3))
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY (market_id, side, price, ts, seq)
SETTINGS index_granularity = 8192
"""

LATEST_LEVELS_DDL = """
CREATE TABLE IF NOT EXISTS {db}.latest_levels
(
    `sid` UInt32,
    `seq` UInt64,
    `market_id` UUID,
    `market_ticker` LowCardinality(String),
    `side` Enum8('yes' = 1, 'no' = 2),
    `price` UInt16,
    `qty` Int32,
    `size` Int32,
    `ts` DateTime64(6, 'UTC'),
    `ingest_ts` DateTime64(6, 'UTC')
)
ENGINE = MergeTree
ORDER BY (market_id, side, price)
"""


# ---------------------------------------------------------------------------
# Helper utilities
# ---------------------------------------------------------------------------

def _execute(client: Client, sql: str) -> None:
    """
    Run a query and log it at DEBUG level. Any ClickHouse errors are propagated.
    """
    log.debug("Executing ClickHouse DDL: %s", sql.strip().replace("\n", " "))
    client.query(sql)


def _column_exists(client: Client, db: str, table: str, column: str) -> bool:
    """
    Check whether a given column exists in system.columns.
    """
    result = client.query(
        """
        SELECT count(*) 
        FROM system.columns 
        WHERE database = %(db)s 
          AND table = %(table)s 
          AND name = %(column)s
        """,
        parameters={"db": db, "table": table, "column": column},
    )
    count = result.result_rows[0][0]
    return count > 0


def _ensure_database(client: Client, db: str) -> None:
    """
    Ensure the database exists.
    """
    _execute(client, f"CREATE DATABASE IF NOT EXISTS {db}")
    log.info("Ensured ClickHouse database '%s' exists", db)


def _ensure_tables(client: Client, db: str) -> None:
    """
    Ensure the core tables exist with at least the minimal required structure.
    """
    _execute(client, ORDERBOOK_EVENTS_DDL.format(db=db))
    log.info("Ensured table %s.orderbook_events exists", db)

    _execute(client, LATEST_LEVELS_DDL.format(db=db))
    log.info("Ensured table %s.latest_levels exists", db)


def _ensure_latest_levels_schema(client: Client, db: str) -> None:
    """
    Ensure that latest_levels has all required columns, including 'size'.

    This is where we patch schema drift between code and DB.
    """
    table = "latest_levels"

    # Make sure the table actually exists (CREATE IF NOT EXISTS is idempotent)
    _execute(client, LATEST_LEVELS_DDL.format(db=db))

    # Ensure 'size' column exists – this is critical because the Python sink
    # passes a 'size' field to latest_levels.
    if not _column_exists(client, db, table, "size"):
        log.warning(
            "Column '%s.%s.%s' is missing; adding it via ALTER TABLE",
            db, table, "size",
        )
        _execute(
            client,
            f"ALTER TABLE {db}.{table} "
            f"ADD COLUMN size Int32 AFTER qty"
        )
        log.info("Added column '%s.%s.%s'", db, table, "size")


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def ensure_clickhouse_schema(
    client: Client,
    db: str = "kalshi",
    extra_tables_ddls: Optional[Iterable[str]] = None,
) -> None:
    """
    Ensure that the ClickHouse schema needed by the KalshiMonorepo exists.

    This function is safe to call on every startup. It will:
      - Create the database if it doesn't exist
      - Create `orderbook_events` and `latest_levels` if they don't exist
      - Ensure `latest_levels.size` exists (required by clickhouse_sink.py)
      - Optionally create any additional tables provided via extra_tables_ddls

    :param client: clickhouse_connect Client instance.
    :param db: Database name to bootstrap (default: 'kalshi').
    :param extra_tables_ddls: Optional iterable of additional CREATE TABLE DDLs
                              that may use `{db}` placeholders.
    """
    try:
        # 1) Ensure DB
        _ensure_database(client, db)

        # 2) Ensure core tables
        _ensure_tables(client, db)

        # 3) Schema drift fixes (e.g., missing 'size' column)
        _ensure_latest_levels_schema(client, db)

        # 4) Any additional tables the caller wants
        if extra_tables_ddls:
            for ddl in extra_tables_ddls:
                _execute(client, ddl.format(db=db))

        log.info("ClickHouse schema ensured for database '%s'", db)

    except (DatabaseError, OperationalError, ProgrammingError) as exc:
        log.exception("Error while bootstrapping ClickHouse schema: %s", exc)
        # Let the caller decide whether this is fatal
        raise
