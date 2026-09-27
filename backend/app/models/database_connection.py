import uuid
from datetime import datetime
from enum import StrEnum

from sqlalchemy import ForeignKey, String, Text, Uuid
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base, TimestampMixin, UTCDateTime
from app.models.airflow import DeploymentEnvironment, _enum
from app.orchestration.airflow.base import ConnectionStatus


class DatabaseEngine(StrEnum):
    POSTGRESQL = "POSTGRESQL"
    MYSQL = "MYSQL"


DEFAULT_PORTS = {DatabaseEngine.POSTGRESQL: 5432, DatabaseEngine.MYSQL: 3306}


class DatabaseConnection(TimestampMixin, Base):
    """A database registered in the connection catalog. Health is checked by the connection
    monitor; nothing else reads from it yet."""

    __tablename__ = "database_connections"

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    name: Mapped[str] = mapped_column(String(100), unique=True)
    environment: Mapped[DeploymentEnvironment] = mapped_column(
        _enum(DeploymentEnvironment, "db_conn_environment")
    )
    engine: Mapped[DatabaseEngine] = mapped_column(_enum(DatabaseEngine, "database_engine"))
    host: Mapped[str] = mapped_column(String(255))
    port: Mapped[int]
    database: Mapped[str] = mapped_column(String(200))
    username: Mapped[str] = mapped_column(String(200))
    encrypted_secret: Mapped[str | None] = mapped_column(Text)
    # PostgreSQL sslmode (disable, prefer, require, verify-ca, verify-full); None = driver default.
    ssl_mode: Mapped[str | None] = mapped_column(String(20))
    is_active: Mapped[bool] = mapped_column(default=True)
    server_version: Mapped[str | None] = mapped_column(String(64))
    last_health_status: Mapped[ConnectionStatus] = mapped_column(
        _enum(ConnectionStatus, "db_conn_status"), default=ConnectionStatus.UNKNOWN
    )
    last_health_message: Mapped[str | None] = mapped_column(Text)
    last_latency_ms: Mapped[int | None]
    last_checked_at: Mapped[datetime | None] = mapped_column(UTCDateTime())
    created_by: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("users.id", ondelete="SET NULL")
    )

    @property
    def has_secret(self) -> bool:
        return bool(self.encrypted_secret)
