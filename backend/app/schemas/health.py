import uuid
from datetime import datetime
from typing import Literal

from pydantic import BaseModel

from app.orchestration.airflow.base import ConnectionStatus


class LivenessResponse(BaseModel):
    status: Literal["ok"] = "ok"


class DatabaseHealth(BaseModel):
    status: Literal["ok", "error"]
    latency_ms: int | None = None
    message: str | None = None


class AirflowConnectionHealth(BaseModel):
    id: uuid.UUID
    name: str
    is_default: bool
    status: ConnectionStatus
    message: str | None
    last_checked_at: datetime | None


class DatabaseConnectionHealth(BaseModel):
    """A database registered in the connection catalog (not the platform's own database)."""

    id: uuid.UUID
    name: str
    engine: str
    status: ConnectionStatus
    message: str | None
    last_checked_at: datetime | None


class HealthStatusResponse(BaseModel):
    status: Literal["ok", "degraded"]
    version: str
    environment: str
    database: DatabaseHealth
    airflow: list[AirflowConnectionHealth]
    databases: list[DatabaseConnectionHealth] = []
