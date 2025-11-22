"""
clickhouse_health.py

Health checks and "fail-fast" utilities for ClickHouse in the KalshiMonorepo.

Goals:
- Verify connectivity
- Verify required DB and tables exist
- Verify there are no "broken parts" (corruption signals) above a threshold
- Provide an assert-style API for mission-critical startup checks
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Iterable, List, Optional

from clickhouse_connect.driver.client import Client
from clickhouse_connect.driver.exceptions import DatabaseError, OperationalError

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class ClickHouseHealthResult:
    ok: bool
    issues: List[str]

    def raise_if_unhealthy(self) -> None:
        if not self.ok:
            raise ClickHouseUnhealthyError(
                "ClickHouse health check failed:\n- "
                + "\n- ".join(self.issues)
            )


class ClickHouseUnhealthyError(RuntimeError):
    """
    Raised when ClickHouse health checks fail in a mission-critical context.
    """
    pass


# ---------------------------------------------------------------------------
# Core health checks
# ---------------------------------------------------------------------------

def check_connectivity(client: Client) -> Optional[str]:
    """
    Check basic connectivity (`SELECT 1`).
    Returns None if OK, or a string describing the issue.
    """
    try:
        client.query("SELECT 1")
        return None
    except (DatabaseError, OperationalError) as exc:
        msg = f"Connectivity check failed: {exc}"
        log.error(msg)
        return msg


def check_database_exists(client: Client, db: str) -> Optional[str]:
    """
    Verify that a database exists.
    """
    try:
        result = client.query(
            "SELECT count(*) FROM system.databases WHERE name = %(db)s",
            parameters={"db": db},
        )
        exists = result.result_rows[0][0] > 0
        if not exists:
            msg = f"Database '{db}' does not exist"
            log.error(msg)
            return msg
        return None
    except (DatabaseError, OperationalError) as exc:
        msg = f"Database existence check failed for '{db}': {exc}"
        log.error(msg)
        return msg


def check_tables_exist(client: Client, db: str, tables: Iterable[str]) -> List[str]:
    """
    Verify that all given tables exist in the specified database.
    Returns a list of issue messages (empty if OK).
    """
    issues: List[str] = []

    for table in tables:
        try:
            result = client.query(
                """
                SELECT count(*) 
                FROM system.tables 
                WHERE database = %(db)s AND name = %(table)s
                """,
                parameters={"db": db, "table": table},
            )
            exists = result.result_rows[0][0] > 0
            if not exists:
                msg = f"Table '{db}.{table}' does not exist"
                log.error(msg)
                issues.append(msg)
        except (DatabaseError, OperationalError) as exc:
            msg = f"Table existence check failed for '{db}.{table}': {exc}"
            log.error(msg)
            issues.append(msg)

    return issues


def check_broken_parts(
    client: Client,
    db: str,
    tables: Iterable[str],
    max_broken_parts: int = 0,
) -> List[str]:
    """
    Check that the number of broken parts in MergeTree tables is <= max_broken_parts.

    A non-zero count of broken parts is often a sign of corruption or rough shutdown.
    For mission-critical contexts, it's reasonable to demand exactly 0.
    """
    table_list = list(tables)
    if not table_list:
        return []

    param_tables = ",".join([f"'{t}'" for t in table_list])

    try:
        result = client.query(
            f"""
            SELECT table, count(*) AS broken_parts
            FROM system.broken_parts
            WHERE database = %(db)s
              AND table IN ({param_tables})
            GROUP BY table
            """,
            parameters={"db": db},
        )
    except (DatabaseError, OperationalError) as exc:
        msg = f"Broken parts check failed: {exc}"
        log.error(msg)
        return [msg]

    issues: List[str] = []
    for table, broken_parts in result.result_rows:
        if broken_parts > max_broken_parts:
            msg = (
                f"Table '{db}.{table}' has {broken_parts} broken parts, "
                f"exceeding allowed max {max_broken_parts}"
            )
            log.error(msg)
            issues.append(msg)

    return issues


# ---------------------------------------------------------------------------
# High-level API
# ---------------------------------------------------------------------------

def evaluate_clickhouse_health(
    client: Client,
    db: str = "kalshi",
    tables: Iterable[str] = ("orderbook_events", "latest_levels"),
    max_broken_parts: int = 0,
) -> ClickHouseHealthResult:
    """
    Run a suite of health checks and return a ClickHouseHealthResult.

    Checks performed:
      - Connectivity
      - Database exists
      - Tables exist
      - Broken parts threshold not exceeded
    """
    issues: List[str] = []

    # 1) Connectivity
    msg = check_connectivity(client)
    if msg:
        issues.append(msg)
        # If we can't even connect, everything else is moot
        return ClickHouseHealthResult(ok=False, issues=issues)

    # 2) Database exists
    msg = check_database_exists(client, db)
    if msg:
        issues.append(msg)
        return ClickHouseHealthResult(ok=False, issues=issues)

    # 3) Tables exist
    issues.extend(check_tables_exist(client, db, tables))

    # 4) Broken parts
    issues.extend(check_broken_parts(client, db, tables, max_broken_parts))

    ok = len(issues) == 0
    if ok:
        log.info("ClickHouse health OK for database '%s', tables=%s", db, list(tables))
    else:
        log.error(
            "ClickHouse health NOT OK for database '%s', tables=%s. Issues:\n- %s",
            db,
            list(tables),
            "\n- ".join(issues),
        )

    return ClickHouseHealthResult(ok=ok, issues=issues)


def assert_clickhouse_ready(
    client: Client,
    db: str = "kalshi",
    tables: Iterable[str] = ("orderbook_events", "latest_levels"),
    max_broken_parts: int = 0,
) -> None:
    """
    Mission-critical convenience wrapper around evaluate_clickhouse_health.

    Call this at startup; it will raise ClickHouseUnhealthyError if any check fails.

    Example:
        client = clickhouse_connect.get_client(...)
        ensure_clickhouse_schema(client)
        assert_clickhouse_ready(client)

    If this raises, you should LOG and EXIT the process rather than try to operate
    on top of a sick ClickHouse instance.
    """
    result = evaluate_clickhouse_health(
        client=client,
        db=db,
        tables=tables,
        max_broken_parts=max_broken_parts,
    )
    result.raise_if_unhealthy()
