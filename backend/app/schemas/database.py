import uuid
from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from app.models.airflow import DeploymentEnvironment
from app.models.database_connection import DatabaseEngine
from app.orchestration.airflow.base import ConnectionStatus

SslMode = Literal["disable", "allow", "prefer", "require", "verify-ca", "verify-full"]


class _TargetFields(BaseModel):
    host: str | None = Field(default=None, min_length=1, max_length=255)
    port: int | None = Field(default=None, ge=1, le=65535)
    database: str | None = Field(default=None, min_length=1, max_length=200)
    username: str | None = Field(default=None, min_length=1, max_length=200)
    secret: str | None = Field(default=None, max_length=4096)
    ssl_mode: SslMode | None = None


class DbConnTestRequest(_TargetFields):
    """Test connection details before saving them."""

    engine: DatabaseEngine
    host: str = Field(min_length=1, max_length=255)
    database: str = Field(min_length=1, max_length=200)
    username: str = Field(min_length=1, max_length=200)


class DbConnCreate(DbConnTestRequest):
    name: str = Field(min_length=1, max_length=100)
    environment: DeploymentEnvironment = DeploymentEnvironment.DEV
    is_active: bool = True


class DbConnUpdate(_TargetFields):
    """Partial update. `secret` present replaces the password (null or "" clears it)."""

    name: str | None = Field(default=None, min_length=1, max_length=100)
    environment: DeploymentEnvironment | None = None
    engine: DatabaseEngine | None = None
    is_active: bool | None = None


class DbConnRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    name: str
    environment: DeploymentEnvironment
    engine: DatabaseEngine
    host: str
    port: int
    database: str
    username: str
    has_secret: bool
    ssl_mode: str | None
    is_active: bool
    server_version: str | None
    last_health_status: ConnectionStatus
    last_health_message: str | None
    last_latency_ms: int | None
    last_checked_at: datetime | None
    created_by: uuid.UUID | None
    created_at: datetime
    updated_at: datetime


class DbConnTestResultRead(BaseModel):
    status: ConnectionStatus
    ok: bool
    message: str
    latency_ms: int | None
    server_version: str | None
    checked_at: datetime
