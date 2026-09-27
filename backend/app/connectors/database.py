"""Database access for the connection catalog: connectivity probes and running SQL.

Opens one short-lived connection, runs `SELECT 1` and reads the server version. Driver errors
are mapped onto the shared `ConnectionStatus` values so the UI treats databases and Airflow alike.
"""

import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, datetime
from datetime import time as time_
from decimal import Decimal
from typing import Any

from sqlalchemy import Engine, create_engine, text
from sqlalchemy.engine import URL
from sqlalchemy.exc import DBAPIError, SQLAlchemyError
from sqlalchemy.pool import NullPool

from app.models.database_connection import DatabaseEngine
from app.orchestration.airflow.base import ConnectionStatus

_DRIVERS = {DatabaseEngine.POSTGRESQL: "postgresql+psycopg", DatabaseEngine.MYSQL: "mysql+pymysql"}

# Tests swap this for one that returns e.g. an in-memory SQLite engine.
engine_factory: Callable[[URL, dict], Engine] | None = None


@dataclass(frozen=True)
class DbTarget:
    engine: DatabaseEngine
    host: str
    port: int
    database: str
    username: str
    secret: str | None
    ssl_mode: str | None = None


@dataclass(frozen=True)
class DbProbeResult:
    status: ConnectionStatus
    message: str
    latency_ms: int | None = None
    server_version: str | None = None

    @property
    def ok(self) -> bool:
        return self.status == ConnectionStatus.HEALTHY


def _url(target: DbTarget) -> URL:
    query = {}
    if target.engine == DatabaseEngine.POSTGRESQL and target.ssl_mode:
        query["sslmode"] = target.ssl_mode
    return URL.create(
        _DRIVERS[target.engine],
        username=target.username,
        password=target.secret,
        host=target.host,
        port=target.port,
        database=target.database,
        query=query,
    )


# Substrings of driver messages (lower-cased), checked in order.
_ERROR_PATTERNS: list[tuple[tuple[str, ...], ConnectionStatus, str]] = [
    (
        ("password authentication failed", "access denied", "no password supplied"),
        ConnectionStatus.UNAUTHORIZED,
        "Authentication failed: check the username and password.",
    ),
    (
        ("permission denied", "not permitted", "insufficient privilege"),
        ConnectionStatus.FORBIDDEN,
        "The user may not connect to this database.",
    ),
    (
        ("ssl", "certificate", "tls"),
        ConnectionStatus.TLS_ERROR,
        "TLS/SSL negotiation failed.",
    ),
    (
        ("does not exist", "unknown database"),
        ConnectionStatus.INVALID_ENDPOINT,
        "The database does not exist on this server.",
    ),
]


def classify_error(exc: Exception) -> tuple[ConnectionStatus, str]:
    detail = str(getattr(exc, "orig", None) or exc).strip().splitlines()
    first_line = detail[0][:300] if detail else type(exc).__name__
    lowered = first_line.lower()
    for needles, status, summary in _ERROR_PATTERNS:
        if any(n in lowered for n in needles):
            return status, f"{summary} ({first_line})"
    return ConnectionStatus.UNREACHABLE, f"Could not connect: {first_line}"


def probe(target: DbTarget, *, timeout_seconds: int) -> DbProbeResult:
    url = _url(target)
    connect_args = {"connect_timeout": timeout_seconds}
    started = time.perf_counter()
    try:
        engine = (engine_factory or _create_engine)(url, connect_args)
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
                info = conn.dialect.server_version_info
        finally:
            engine.dispose()
    except (DBAPIError, SQLAlchemyError) as exc:
        status, message = classify_error(exc)
        return DbProbeResult(status=status, message=message)
    except Exception as exc:  # e.g. a driver bug; must not break the monitor cycle
        return DbProbeResult(
            status=ConnectionStatus.UNREACHABLE, message=f"Could not connect: {exc}"
        )
    version = ".".join(str(p) for p in info) if info else None
    return DbProbeResult(
        status=ConnectionStatus.HEALTHY,
        message="Connected",
        latency_ms=int((time.perf_counter() - started) * 1000),
        server_version=version,
    )


def _create_engine(url: URL, connect_args: dict) -> Engine:
    return create_engine(url, poolclass=NullPool, connect_args=connect_args)


# ---------------------------------------------------------------------- running SQL


class SqlError(Exception):
    """The statement could not run (connection, permission, syntax, timeout, ...)."""


@dataclass(frozen=True)
class SqlResult:
    columns: list[str]
    rows: list[list[Any]]  # at most `max_rows`, JSON-safe values
    row_count: int  # rows returned (capped at max_rows + 1) or rows affected
    returns_rows: bool
    truncated: bool


def _jsonable(value: Any) -> Any:
    if value is None or isinstance(value, bool | int | float | str):
        return value
    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() else float(value)
    if isinstance(value, datetime | date | time_):
        return value.isoformat()
    if isinstance(value, bytes | bytearray | memoryview):
        return f"<{len(bytes(value))} bytes>"
    return str(value)


def _session_setup(conn: Any, *, read_only: bool, timeout_seconds: int) -> None:
    """Per-dialect read-only mode and statement timeout for this one transaction."""
    dialect = conn.dialect.name
    if dialect == "postgresql":
        if read_only:
            conn.exec_driver_sql("SET TRANSACTION READ ONLY")
        conn.exec_driver_sql(f"SET LOCAL statement_timeout = {int(timeout_seconds) * 1000}")
    elif dialect == "mysql":
        if read_only:
            conn.exec_driver_sql("SET TRANSACTION READ ONLY")
        conn.exec_driver_sql(f"SET SESSION max_execution_time = {int(timeout_seconds) * 1000}")
    elif dialect == "sqlite" and read_only:
        conn.exec_driver_sql("PRAGMA query_only = ON")


def execute(
    target: DbTarget, sql: str, *, read_only: bool, timeout_seconds: int, max_rows: int = 20
) -> SqlResult:
    """Run one SQL statement in its own transaction and commit it (unless read-only).

    The SQL is sent as-is (no parameter parsing), exactly as an operator would type it.
    """
    connect_args = {"connect_timeout": min(int(timeout_seconds), 30)}
    try:
        engine = (engine_factory or _create_engine)(_url(target), connect_args)
        try:
            with engine.connect() as conn, conn.begin():
                _session_setup(conn, read_only=read_only, timeout_seconds=timeout_seconds)
                result = conn.exec_driver_sql(sql)
                if result.returns_rows:
                    columns = list(result.keys())
                    fetched = result.fetchmany(max_rows + 1)
                    rows = [[_jsonable(v) for v in row] for row in fetched[:max_rows]]
                    return SqlResult(
                        columns=columns,
                        rows=rows,
                        row_count=len(fetched),
                        returns_rows=True,
                        truncated=len(fetched) > max_rows,
                    )
                return SqlResult(
                    columns=[],
                    rows=[],
                    row_count=max(result.rowcount, 0),
                    returns_rows=False,
                    truncated=False,
                )
        finally:
            engine.dispose()
    except (DBAPIError, SQLAlchemyError) as exc:
        detail = str(getattr(exc, "orig", None) or exc).strip().splitlines()
        raise SqlError(detail[0][:500] if detail else type(exc).__name__) from exc
