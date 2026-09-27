"""Database connections in the connection catalog: CRUD, connection tests and health checks."""

import logging
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.connectors import database as db_connector
from app.connectors.database import DbProbeResult, DbTarget
from app.core.config import get_settings
from app.core.crypto import SecretDecryptionError, decrypt_secret, encrypt_secret
from app.core.exceptions import AppError, BadRequestError, ConflictError, NotFoundError
from app.db.base import utcnow
from app.models.database_connection import DEFAULT_PORTS, DatabaseConnection, DatabaseEngine
from app.models.user import User
from app.orchestration.airflow.base import ConnectionStatus
from app.schemas.database import DbConnCreate, DbConnTestRequest, DbConnUpdate
from app.services import audit_service

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DbProbeOutcome:
    result: DbProbeResult
    checked_at: datetime


@dataclass
class DbCheckSummary:
    healthy: int = 0
    failing: int = 0
    transitions: list[dict[str, str]] = field(default_factory=list)


# ---------------------------------------------------------------------- targets


def _guard_host(host: str) -> None:
    allowed = {h.lower() for h in get_settings().DB_CONNECTION_ALLOWED_HOSTS}
    if allowed and host.lower() not in allowed:
        raise BadRequestError(
            f"Host {host!r} is not in DB_CONNECTION_ALLOWED_HOSTS", code="host_not_allowed"
        )


def _probe(target: DbTarget) -> DbProbeResult:
    return db_connector.probe(target, timeout_seconds=get_settings().DB_CONNECT_TIMEOUT_SECONDS)


def target_for(conn: DatabaseConnection) -> DbTarget:
    """Connection details (decrypted secret) for a stored connection; checks the host allowlist."""
    _guard_host(conn.host)
    try:
        secret = decrypt_secret(conn.encrypted_secret) if conn.encrypted_secret else None
    except SecretDecryptionError as exc:
        raise BadRequestError(
            "The stored password cannot be decrypted (was ENCRYPTION_KEY changed?). "
            "Re-enter it on the connection.",
            code="secret_undecryptable",
        ) from exc
    return DbTarget(
        engine=conn.engine,
        host=conn.host,
        port=conn.port,
        database=conn.database,
        username=conn.username,
        secret=secret,
        ssl_mode=conn.ssl_mode,
    )


def _store_health(conn: DatabaseConnection, result: DbProbeResult, when: datetime) -> None:
    conn.last_health_status = result.status
    conn.last_health_message = result.message
    conn.last_latency_ms = result.latency_ms
    conn.last_checked_at = when
    if result.ok:
        conn.server_version = result.server_version


def _ssl_mode(engine: DatabaseEngine, ssl_mode: str | None) -> str | None:
    return ssl_mode if engine == DatabaseEngine.POSTGRESQL else None


# ---------------------------------------------------------------------- connections


def list_connections(
    db: Session, *, limit: int, offset: int
) -> tuple[list[DatabaseConnection], int]:
    total = db.scalar(select(func.count()).select_from(DatabaseConnection)) or 0
    items = db.scalars(
        select(DatabaseConnection).order_by(DatabaseConnection.name).limit(limit).offset(offset)
    ).all()
    return list(items), total


def get_connection(db: Session, connection_id: uuid.UUID) -> DatabaseConnection:
    conn = db.get(DatabaseConnection, connection_id)
    if conn is None:
        raise NotFoundError("Database connection not found")
    return conn


def _ensure_unique_name(db: Session, name: str, exclude_id: uuid.UUID | None = None) -> None:
    query = select(DatabaseConnection.id).where(func.lower(DatabaseConnection.name) == name.lower())
    if exclude_id:
        query = query.where(DatabaseConnection.id != exclude_id)
    if db.scalar(query):
        raise ConflictError(f"A database connection named {name!r} already exists")


def create_connection(
    db: Session, data: DbConnCreate, *, actor: User, ip_address: str | None
) -> DatabaseConnection:
    _guard_host(data.host)
    _ensure_unique_name(db, data.name)
    conn = DatabaseConnection(
        id=uuid.uuid4(),
        name=data.name,
        environment=data.environment,
        engine=data.engine,
        host=data.host,
        port=data.port or DEFAULT_PORTS[data.engine],
        database=data.database,
        username=data.username,
        encrypted_secret=encrypt_secret(data.secret) if data.secret else None,
        ssl_mode=_ssl_mode(data.engine, data.ssl_mode),
        is_active=data.is_active,
        created_by=actor.id,
    )
    db.add(conn)
    db.flush()
    audit_service.record(
        db,
        action="db_conn.create",
        entity_type="database_connection",
        entity_id=conn.id,
        actor=actor,
        details={
            "name": conn.name,
            "engine": conn.engine,
            "host": conn.host,
            "port": conn.port,
            "database": conn.database,
            "environment": conn.environment,
        },
        ip_address=ip_address,
    )
    db.commit()
    return conn


_TARGET_FIELDS = ("engine", "host", "port", "database", "username", "ssl_mode")


def update_connection(
    db: Session,
    connection_id: uuid.UUID,
    data: DbConnUpdate,
    *,
    actor: User,
    ip_address: str | None,
) -> DatabaseConnection:
    conn = get_connection(db, connection_id)
    changes = data.model_dump(exclude_unset=True)
    secret_provided = "secret" in changes
    secret = changes.pop("secret", None)
    if changes.get("name"):
        _ensure_unique_name(db, changes["name"], exclude_id=conn.id)

    changed: dict[str, object] = {}
    for name, value in changes.items():
        if value is None and name != "ssl_mode":
            continue  # null means "unchanged", except ssl_mode where it resets to the default
        if getattr(conn, name) != value:
            changed[name] = {"from": getattr(conn, name), "to": value}
            setattr(conn, name, value)
    conn.ssl_mode = _ssl_mode(conn.engine, conn.ssl_mode)
    if secret_provided:
        conn.encrypted_secret = encrypt_secret(secret) if secret else None
        changed["secret_changed"] = True
    _guard_host(conn.host)

    if any(f in changed for f in (*_TARGET_FIELDS, "secret_changed")):
        # Target or credentials changed: the previous health result no longer applies.
        conn.server_version = None
        conn.last_health_status = ConnectionStatus.UNKNOWN
        conn.last_health_message = None
        conn.last_latency_ms = None
        conn.last_checked_at = None

    if changed:
        audit_service.record(
            db,
            action="db_conn.update",
            entity_type="database_connection",
            entity_id=conn.id,
            actor=actor,
            details={"changes": changed},
            ip_address=ip_address,
        )
    db.commit()
    return conn


def delete_connection(
    db: Session, connection_id: uuid.UUID, *, actor: User, ip_address: str | None
) -> None:
    conn = get_connection(db, connection_id)
    audit_service.record(
        db,
        action="db_conn.delete",
        entity_type="database_connection",
        entity_id=conn.id,
        actor=actor,
        details={"name": conn.name, "host": conn.host, "database": conn.database},
        ip_address=ip_address,
    )
    db.delete(conn)
    db.commit()


# ---------------------------------------------------------------------- testing


def check_unsaved_connection(
    db: Session, data: DbConnTestRequest, *, actor: User, ip_address: str | None
) -> DbProbeOutcome:
    _guard_host(data.host)
    result = _probe(
        DbTarget(
            engine=data.engine,
            host=data.host,
            port=data.port or DEFAULT_PORTS[data.engine],
            database=data.database,
            username=data.username,
            secret=data.secret,
            ssl_mode=_ssl_mode(data.engine, data.ssl_mode),
        )
    )
    audit_service.record(
        db,
        action="db_conn.test",
        entity_type="database_connection",
        actor=actor,
        details={"host": data.host, "engine": data.engine, "status": result.status},
        ip_address=ip_address,
    )
    db.commit()
    return DbProbeOutcome(result=result, checked_at=utcnow())


def check_saved_connection(
    db: Session, connection_id: uuid.UUID, *, actor: User, ip_address: str | None
) -> DbProbeOutcome:
    conn = get_connection(db, connection_id)
    result = _probe_saved(conn)
    checked_at = utcnow()
    _store_health(conn, result, checked_at)
    audit_service.record(
        db,
        action="db_conn.test",
        entity_type="database_connection",
        entity_id=conn.id,
        actor=actor,
        details={"status": result.status, "latency_ms": result.latency_ms},
        ip_address=ip_address,
    )
    db.commit()
    return DbProbeOutcome(result=result, checked_at=checked_at)


# ---------------------------------------------------------------------- monitor


def _probe_saved(conn: DatabaseConnection) -> DbProbeResult:
    """Probe a stored connection; unusable stored config becomes a failed result."""
    try:
        return _probe(target_for(conn))
    except AppError as exc:
        status = (
            ConnectionStatus.UNAUTHORIZED
            if exc.code == "secret_undecryptable"
            else ConnectionStatus.INVALID_ENDPOINT
        )
        return DbProbeResult(status=status, message=exc.message)


def check_all(db: Session, *, actor: User | None = None) -> DbCheckSummary:
    """Probe every active database connection concurrently and record the results. Commits."""
    summary = DbCheckSummary()
    conns = db.scalars(
        select(DatabaseConnection)
        .where(DatabaseConnection.is_active.is_(True))
        .order_by(DatabaseConnection.name)
    ).all()
    if not conns:
        return summary
    with ThreadPoolExecutor(max_workers=min(8, len(conns))) as pool:
        results = list(pool.map(_probe_saved, conns))
    now = utcnow()
    for conn, result in zip(conns, results, strict=True):
        before = conn.last_health_status
        _store_health(conn, result, now)
        if result.ok:
            summary.healthy += 1
        else:
            summary.failing += 1
        if before != result.status:
            audit_service.record(
                db,
                action="db_conn.status_changed",
                entity_type="database_connection",
                entity_id=conn.id,
                actor=actor,
                details={"from": before, "to": result.status, "message": result.message},
            )
            summary.transitions.append(
                {"connection": conn.name, "from": before, "to": result.status}
            )
            log = logger.info if result.ok else logger.warning
            log("Database connection %s: %s -> %s", conn.name, before, result.status)
    db.commit()
    return summary
