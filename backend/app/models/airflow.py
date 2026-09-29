import uuid
from datetime import datetime
from enum import StrEnum

from sqlalchemy import (
    JSON,
    Enum,
    ForeignKey,
    Index,
    String,
    Text,
    UniqueConstraint,
    Uuid,
    text,
    true,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.core.config import get_settings
from app.db.base import Base, TimestampMixin, UTCDateTime, utcnow
from app.orchestration.airflow.base import ApiVersion, AuthType, ConnectionStatus


class ConnectionKind(StrEnum):
    LIVE = "LIVE"
    MOCK = "MOCK"


class DeploymentEnvironment(StrEnum):
    DEV = "DEV"
    STAGING = "STAGING"
    PROD = "PROD"


def _enum(enum_cls: type[StrEnum], name: str, length: int = 32) -> Enum:
    return Enum(enum_cls, native_enum=False, length=length, create_constraint=True, name=name)


class AirflowConnection(TimestampMixin, Base):
    __tablename__ = "airflow_connections"
    __table_args__ = (
        # At most one default connection.
        Index(
            "uq_airflow_connections_single_default",
            "is_default",
            unique=True,
            sqlite_where=text("is_default = 1"),
            postgresql_where=text("is_default = true"),
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    name: Mapped[str] = mapped_column(String(100), unique=True)
    environment: Mapped[DeploymentEnvironment] = mapped_column(
        _enum(DeploymentEnvironment, "deployment_environment")
    )
    kind: Mapped[ConnectionKind] = mapped_column(_enum(ConnectionKind, "connection_kind"))
    base_url: Mapped[str] = mapped_column(String(500))
    auth_type: Mapped[AuthType] = mapped_column(_enum(AuthType, "auth_type"))
    username: Mapped[str | None] = mapped_column(String(200))
    encrypted_secret: Mapped[str | None] = mapped_column(Text)
    api_version: Mapped[ApiVersion | None] = mapped_column(_enum(ApiVersion, "api_version", 8))
    airflow_version: Mapped[str | None] = mapped_column(String(32))
    is_active: Mapped[bool] = mapped_column(default=True)
    is_default: Mapped[bool] = mapped_column(default=False)
    last_health_status: Mapped[ConnectionStatus] = mapped_column(
        _enum(ConnectionStatus, "connection_status"), default=ConnectionStatus.UNKNOWN
    )
    last_health_message: Mapped[str | None] = mapped_column(Text)
    last_latency_ms: Mapped[int | None]
    last_checked_at: Mapped[datetime | None] = mapped_column(UTCDateTime())
    created_by: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("users.id", ondelete="SET NULL")
    )

    dags: Mapped[list["MonitoredDag"]] = relationship(
        back_populates="connection", cascade="all, delete-orphan", passive_deletes=True
    )

    @property
    def has_secret(self) -> bool:
        return bool(self.encrypted_secret)

    @property
    def status_stale(self) -> bool:
        """True when the connection monitor should have re-checked this connection but has not."""
        settings = get_settings()
        if not settings.AIRFLOW_MONITOR_ENABLED or not self.is_active:
            return False
        # Never checked (new or just edited): give the monitor one window to get to it.
        reference = self.last_checked_at or self.updated_at
        if reference is None:
            return False
        # Two monitor intervals plus one worst-case Airflow call.
        max_age = (
            2 * settings.AIRFLOW_MONITOR_INTERVAL_SECONDS
            + settings.AIRFLOW_CONNECT_TIMEOUT_SECONDS
            + settings.AIRFLOW_READ_TIMEOUT_SECONDS
        )
        return (utcnow() - reference).total_seconds() > max_age

    @property
    def is_live(self) -> bool:
        """Airflow answered recently enough that its DAG list can be shown as current."""
        return (
            self.is_active
            and self.last_health_status == ConnectionStatus.HEALTHY
            and not self.status_stale
        )


class MonitoredDag(TimestampMixin, Base):
    __tablename__ = "monitored_dags"
    __table_args__ = (UniqueConstraint("connection_id", "dag_id"),)

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    connection_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("airflow_connections.id", ondelete="CASCADE"), index=True
    )
    dag_id: Mapped[str] = mapped_column(String(250))
    description: Mapped[str | None] = mapped_column(Text)
    schedule_summary: Mapped[str | None] = mapped_column(String(200))
    is_paused: Mapped[bool | None]
    is_monitored: Mapped[bool] = mapped_column(default=False)
    is_present: Mapped[bool] = mapped_column(default=True)
    tags: Mapped[list[str]] = mapped_column(JSON, default=list)
    last_synced_at: Mapped[datetime | None] = mapped_column(UTCDateTime())
    # Detection: freshness SLA (None = no SLA) and the newest terminal run already processed.
    sla_minutes: Mapped[int | None]
    # Plan 3: open DAG_RUN_FAILED incidents (the canvas "Failure monitor" block).
    detect_failures: Mapped[bool] = mapped_column(default=True, server_default=true())
    detection_watermark: Mapped[datetime | None] = mapped_column(UTCDateTime())

    connection: Mapped[AirflowConnection] = relationship(back_populates="dags")


class AirflowTriggerReservation(TimestampMixin, Base):
    """Database guard for a DAG trigger that may still be active in Airflow."""

    __tablename__ = "airflow_trigger_reservations"
    __table_args__ = (
        UniqueConstraint("connection_id", "dag_id"),
        Index("ix_airflow_trigger_reservations_workflow_run", "workflow_run_id"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    connection_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("airflow_connections.id", ondelete="CASCADE"), index=True
    )
    dag_id: Mapped[str] = mapped_column(String(250))
    workflow_run_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("workflow_runs.id", ondelete="CASCADE"), index=True
    )
    node_id: Mapped[str] = mapped_column(String(100))
    airflow_run_id: Mapped[str | None] = mapped_column(String(250))
