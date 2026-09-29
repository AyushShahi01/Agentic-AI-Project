import logging
import time
import uuid
from dataclasses import dataclass
from datetime import datetime
from urllib.parse import urlsplit

from sqlalchemy import func, or_, select, update
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.core.crypto import SecretDecryptionError, decrypt_secret, encrypt_secret
from app.core.exceptions import BadRequestError, ConflictError, NotFoundError, UpstreamError
from app.db.base import utcnow
from app.models.airflow import AirflowConnection, ConnectionKind, MonitoredDag
from app.models.user import User
from app.orchestration.airflow.base import (
    AdapterConfig,
    AirflowAdapter,
    AirflowAdapterError,
    AirflowDagSummary,
    ApiVersion,
    AuthType,
    ConnectionStatus,
    ConnectionTestResult,
    normalize_base_url,
)
from app.orchestration.airflow.factory import build_adapter, run_async
from app.schemas.airflow import AirflowConnCreate, AirflowConnTestRequest, AirflowConnUpdate
from app.services import audit_service

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ProbeOutcome:
    result: ConnectionTestResult
    checked_at: datetime


@dataclass(frozen=True)
class SyncOutcome:
    total: int
    created: int
    updated: int
    missing: int
    synced_at: datetime


# ---------------------------------------------------------------------- guards


def _guard_target(kind: ConnectionKind, base_url: str) -> None:
    settings = get_settings()
    if kind == ConnectionKind.MOCK:
        if settings.ENVIRONMENT == "production":
            raise BadRequestError("Mock connections are disabled in production")
        return
    allowed = {h.lower() for h in settings.AIRFLOW_ALLOWED_HOSTS}
    host = (urlsplit(base_url).hostname or "").lower()
    if allowed and host not in allowed:
        raise BadRequestError(
            f"Host {host!r} is not in AIRFLOW_ALLOWED_HOSTS", code="host_not_allowed"
        )


def _require_credentials(
    kind: ConnectionKind, auth_type: AuthType, username: str | None, has_secret: bool
) -> None:
    if kind != ConnectionKind.LIVE or auth_type == AuthType.NONE:
        return
    if auth_type == AuthType.BASIC and not username:
        raise BadRequestError("username is required for BASIC auth")
    if not has_secret:
        raise BadRequestError("A password or token is required for BASIC and TOKEN auth")


# ---------------------------------------------------------------------- adapters


def _adapter_config(
    *,
    base_url: str,
    auth_type: AuthType,
    username: str | None,
    secret: str | None,
    api_version: ApiVersion | None = None,
) -> AdapterConfig:
    settings = get_settings()
    return AdapterConfig(
        base_url=base_url,
        auth_type=auth_type,
        username=username,
        secret=secret,
        api_version=api_version,
        connect_timeout=settings.AIRFLOW_CONNECT_TIMEOUT_SECONDS,
        read_timeout=settings.AIRFLOW_READ_TIMEOUT_SECONDS,
    )


def adapter_for(conn: AirflowConnection) -> AirflowAdapter:
    _guard_target(conn.kind, conn.base_url)
    try:
        secret = decrypt_secret(conn.encrypted_secret) if conn.encrypted_secret else None
    except SecretDecryptionError as exc:
        raise BadRequestError(
            "The stored password/token cannot be decrypted (was ENCRYPTION_KEY changed?). "
            "Re-enter it on the connection.",
            code="secret_undecryptable",
        ) from exc
    config = _adapter_config(
        base_url=conn.base_url,
        auth_type=conn.auth_type,
        username=conn.username,
        secret=secret,
        api_version=conn.api_version,
    )
    return build_adapter(mock=conn.kind == ConnectionKind.MOCK, config=config)


# ---------------------------------------------------------------------- connections


def list_connections(
    db: Session, *, limit: int, offset: int
) -> tuple[list[AirflowConnection], int]:
    total = db.scalar(select(func.count()).select_from(AirflowConnection)) or 0
    items = db.scalars(
        select(AirflowConnection)
        .order_by(AirflowConnection.is_default.desc(), AirflowConnection.name)
        .limit(limit)
        .offset(offset)
    ).all()
    return list(items), total


def get_connection(db: Session, connection_id: uuid.UUID) -> AirflowConnection:
    conn = db.get(AirflowConnection, connection_id)
    if conn is None:
        raise NotFoundError("Airflow connection not found")
    return conn


def _ensure_unique_name(db: Session, name: str, exclude_id: uuid.UUID | None = None) -> None:
    query = select(AirflowConnection.id).where(func.lower(AirflowConnection.name) == name.lower())
    if exclude_id:
        query = query.where(AirflowConnection.id != exclude_id)
    if db.scalar(query):
        raise ConflictError(f"A connection named {name!r} already exists")


def _clear_other_defaults(db: Session, keep_id: uuid.UUID) -> None:
    db.execute(
        update(AirflowConnection)
        .where(AirflowConnection.is_default.is_(True), AirflowConnection.id != keep_id)
        .values(is_default=False)
    )


def create_connection(
    db: Session, data: AirflowConnCreate, *, actor: User, ip_address: str | None
) -> AirflowConnection:
    _guard_target(data.kind, data.base_url)
    _ensure_unique_name(db, data.name)
    conn = AirflowConnection(
        id=uuid.uuid4(),
        name=data.name,
        environment=data.environment,
        kind=data.kind,
        base_url=data.base_url,
        auth_type=data.auth_type,
        username=data.username or None,
        encrypted_secret=encrypt_secret(data.secret) if data.secret else None,
        is_active=data.is_active,
        is_default=False,
        created_by=actor.id,
    )
    db.add(conn)
    db.flush()
    if data.is_default:
        _clear_other_defaults(db, conn.id)
        conn.is_default = True
    audit_service.record(
        db,
        action="airflow_conn.create",
        entity_type="airflow_connection",
        entity_id=conn.id,
        actor=actor,
        details={
            "name": conn.name,
            "kind": conn.kind,
            "base_url": conn.base_url,
            "auth_type": conn.auth_type,
            "environment": conn.environment,
        },
        ip_address=ip_address,
    )
    db.commit()
    return conn


_TARGET_FIELDS = ("kind", "base_url", "auth_type", "username")


def update_connection(
    db: Session,
    connection_id: uuid.UUID,
    data: AirflowConnUpdate,
    *,
    actor: User,
    ip_address: str | None,
) -> AirflowConnection:
    conn = get_connection(db, connection_id)
    changes = data.model_dump(exclude_unset=True)
    secret_provided = "secret" in changes
    secret = changes.pop("secret", None)

    kind = changes.get("kind") or conn.kind
    if "base_url" in changes or "kind" in changes:
        raw_url = changes.get("base_url") or conn.base_url
        try:
            changes["base_url"] = normalize_base_url(
                raw_url, allow_query=kind == ConnectionKind.MOCK
            )
        except ValueError as exc:
            raise BadRequestError(str(exc)) from exc
    if "name" in changes and changes["name"]:
        _ensure_unique_name(db, changes["name"], exclude_id=conn.id)

    changed: dict[str, object] = {}
    for field, value in changes.items():
        if field == "is_default":
            continue  # handled below
        if value is None and field != "username":
            continue  # null means "unchanged" except for username, where it clears
        if getattr(conn, field) != value:
            changed[field] = {"from": getattr(conn, field), "to": value}
            setattr(conn, field, value)
    if secret_provided:
        conn.encrypted_secret = encrypt_secret(secret) if secret else None
        changed["secret_changed"] = True

    _require_credentials(conn.kind, conn.auth_type, conn.username, conn.has_secret)
    _guard_target(conn.kind, conn.base_url)

    if changes.get("is_default") is not None and changes["is_default"] != conn.is_default:
        if changes["is_default"]:
            _clear_other_defaults(db, conn.id)
            db.flush()
        conn.is_default = changes["is_default"]
        changed["is_default"] = conn.is_default

    if any(f in changed for f in (*_TARGET_FIELDS, "secret_changed")):
        # Target or credentials changed: previous health/version results no longer apply.
        conn.api_version = None
        conn.airflow_version = None
        conn.last_health_status = ConnectionStatus.UNKNOWN
        conn.last_health_message = None
        conn.last_latency_ms = None
        conn.last_checked_at = None

    if changed:
        audit_service.record(
            db,
            action="airflow_conn.update",
            entity_type="airflow_connection",
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
        action="airflow_conn.delete",
        entity_type="airflow_connection",
        entity_id=conn.id,
        actor=actor,
        details={"name": conn.name, "base_url": conn.base_url},
        ip_address=ip_address,
    )
    db.delete(conn)
    db.commit()


# ---------------------------------------------------------------------- testing


def check_unsaved_connection(
    db: Session, data: AirflowConnTestRequest, *, actor: User, ip_address: str | None
) -> ProbeOutcome:
    _guard_target(data.kind, data.base_url)
    _require_credentials(data.kind, data.auth_type, data.username, bool(data.secret))
    adapter = build_adapter(
        mock=data.kind == ConnectionKind.MOCK,
        config=_adapter_config(
            base_url=data.base_url,
            auth_type=data.auth_type,
            username=data.username,
            secret=data.secret,
        ),
    )
    result = run_async(adapter.probe)
    audit_service.record(
        db,
        action="airflow_conn.test",
        entity_type="airflow_connection",
        actor=actor,
        details={"base_url": data.base_url, "kind": data.kind, "status": result.status},
        ip_address=ip_address,
    )
    db.commit()
    return ProbeOutcome(result=result, checked_at=utcnow())


def store_health(conn: AirflowConnection, result: ConnectionTestResult, when: datetime) -> None:
    conn.last_health_status = result.status
    conn.last_health_message = result.message
    conn.last_latency_ms = result.latency_ms
    conn.last_checked_at = when
    if result.ok:
        conn.api_version = result.api_version
        conn.airflow_version = result.airflow_version


def mark_healthy(conn: AirflowConnection, latency_ms: int | None, when: datetime) -> None:
    """Record a successful live call that was not a probe (e.g. listing DAGs)."""
    conn.last_health_status = ConnectionStatus.HEALTHY
    conn.last_health_message = "Connected"
    conn.last_latency_ms = latency_ms
    conn.last_checked_at = when


def dag_sync_due(conn: AirflowConnection, now: datetime | None = None) -> bool:
    """Return whether the monitor needs a fresh DAG list for this connection."""
    settings = get_settings()
    if not conn.is_active:
        return False
    synced = [dag.last_synced_at for dag in conn.dags if dag.last_synced_at is not None]
    reference = max(synced, default=None)
    if reference is None:
        return True
    now = now or utcnow()
    due = (now - reference).total_seconds() >= settings.AIRFLOW_DAG_SYNC_MIN_INTERVAL_SECONDS
    if not due:
        logger.debug(
            "Skipped Airflow DAG sync for %s; last sync was %.1fs ago",
            conn.name,
            (now - reference).total_seconds(),
        )
    return due


def check_saved_connection(
    db: Session, connection_id: uuid.UUID, *, actor: User, ip_address: str | None
) -> ProbeOutcome:
    conn = get_connection(db, connection_id)
    result = run_async(adapter_for(conn).probe)
    checked_at = utcnow()
    store_health(conn, result, checked_at)
    audit_service.record(
        db,
        action="airflow_conn.test",
        entity_type="airflow_connection",
        entity_id=conn.id,
        actor=actor,
        details={"status": result.status, "latency_ms": result.latency_ms},
        ip_address=ip_address,
    )
    db.commit()
    return ProbeOutcome(result=result, checked_at=checked_at)


# ---------------------------------------------------------------------- DAGs


def sync_dags(
    db: Session, connection_id: uuid.UUID, *, actor: User, ip_address: str | None
) -> SyncOutcome:
    conn = get_connection(db, connection_id)
    if not conn.is_active:
        raise BadRequestError("Connection is inactive; activate it before syncing DAGs")
    adapter = adapter_for(conn)
    now = utcnow()
    started = time.perf_counter()
    logger.debug("Starting Airflow DAG sync for %s", conn.name)
    try:
        remote_dags = run_async(adapter.list_dags)
    except AirflowAdapterError as exc:
        store_health(conn, exc.result, now)
        audit_service.record(
            db,
            action="airflow_dags.sync",
            entity_type="airflow_connection",
            entity_id=conn.id,
            actor=actor,
            details={"ok": False, "status": exc.result.status},
            ip_address=ip_address,
        )
        db.commit()
        raise UpstreamError(
            exc.result.message,
            code="airflow_unavailable",
            details={"status": exc.result.status},
        ) from exc

    latency_ms = int((time.perf_counter() - started) * 1000)
    mark_healthy(conn, latency_ms, now)
    created, updated, missing = apply_remote_dags(db, conn, remote_dags, now)

    audit_service.record(
        db,
        action="airflow_dags.sync",
        entity_type="airflow_connection",
        entity_id=conn.id,
        actor=actor,
        details={
            "ok": True,
            "total": len(remote_dags),
            "created": created,
            "updated": updated,
            "missing": missing,
        },
        ip_address=ip_address,
    )
    db.commit()
    return SyncOutcome(
        total=len(remote_dags), created=created, updated=updated, missing=missing, synced_at=now
    )


def apply_remote_dags(
    db: Session, conn: AirflowConnection, remote_dags: list[AirflowDagSummary], now: datetime
) -> tuple[int, int, int]:
    """Reconcile Airflow's DAG list into `monitored_dags`; returns (created, updated, missing)."""
    existing = {d.dag_id: d for d in conn.dags}
    seen: set[str] = set()
    created = updated = 0
    for remote in remote_dags:
        seen.add(remote.dag_id)
        dag = existing.get(remote.dag_id)
        if dag is None:
            dag = MonitoredDag(dag_id=remote.dag_id, is_monitored=False)
            conn.dags.append(dag)  # keeps `conn.dags` current for later calls in this session
            created += 1
        else:
            updated += 1
        dag.description = remote.description
        dag.schedule_summary = remote.schedule_summary
        dag.is_paused = remote.is_paused
        dag.tags = list(remote.tags)
        dag.is_present = True
        dag.last_synced_at = now

    missing = 0
    for dag_id, dag in existing.items():
        if dag_id not in seen and dag.is_present:
            dag.is_present = False
            missing += 1
    return created, updated, missing


def list_dags(
    db: Session,
    connection_id: uuid.UUID,
    *,
    monitored: bool | None,
    tag: str | None,
    search: str | None,
    limit: int,
    offset: int,
) -> tuple[list[MonitoredDag], int]:
    get_connection(db, connection_id)
    query = select(MonitoredDag).where(MonitoredDag.connection_id == connection_id)
    if monitored is not None:
        query = query.where(MonitoredDag.is_monitored.is_(monitored))
    if search:
        pattern = f"%{search.lower()}%"
        query = query.where(
            or_(
                func.lower(MonitoredDag.dag_id).like(pattern),
                func.lower(MonitoredDag.description).like(pattern),
            )
        )
    dags = list(db.scalars(query.order_by(MonitoredDag.dag_id)).all())
    if tag:
        dags = [d for d in dags if tag in (d.tags or [])]  # JSON filtering kept DB-portable
    return dags[offset : offset + limit], len(dags)


def update_dag(
    db: Session,
    dag_pk: uuid.UUID,
    changes: dict[str, object],
    *,
    actor: User,
    ip_address: str | None,
) -> MonitoredDag:
    """Apply `is_monitored`, `sla_minutes` and/or `detect_failures` (keys in `changes`)."""
    dag = db.get(MonitoredDag, dag_pk)
    if dag is None:
        raise NotFoundError("DAG not found")
    base_details = {"connection_id": str(dag.connection_id), "dag_id": dag.dag_id}
    if "is_monitored" in changes and changes["is_monitored"] is not None:
        is_monitored = bool(changes["is_monitored"])
        if dag.is_monitored != is_monitored:
            dag.is_monitored = is_monitored
            audit_service.record(
                db,
                action="monitored_dag.toggle",
                entity_type="monitored_dag",
                entity_id=dag.id,
                actor=actor,
                details={**base_details, "is_monitored": is_monitored},
                ip_address=ip_address,
            )
    detect = changes.get("detect_failures")
    if detect is not None and bool(detect) != dag.detect_failures:
        dag.detect_failures = bool(detect)
        audit_service.record(
            db,
            action="monitored_dag.failure_monitor",
            entity_type="monitored_dag",
            entity_id=dag.id,
            actor=actor,
            details={**base_details, "detect_failures": dag.detect_failures},
            ip_address=ip_address,
        )
    if "sla_minutes" in changes and changes["sla_minutes"] != dag.sla_minutes:
        previous = dag.sla_minutes
        dag.sla_minutes = changes["sla_minutes"]  # type: ignore[assignment]
        audit_service.record(
            db,
            action="monitored_dag.sla_update",
            entity_type="monitored_dag",
            entity_id=dag.id,
            actor=actor,
            details={**base_details, "sla_minutes": {"from": previous, "to": dag.sla_minutes}},
            ip_address=ip_address,
        )
    db.commit()
    return dag
