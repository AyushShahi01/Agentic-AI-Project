import time

from fastapi import APIRouter
from sqlalchemy import select, text
from sqlalchemy.exc import SQLAlchemyError

from app.core.config import get_settings
from app.core.dependencies import DbSession, ViewerUser
from app.models.airflow import AirflowConnection
from app.orchestration.airflow.base import ConnectionStatus
from app.schemas.health import (
    AirflowConnectionHealth,
    DatabaseHealth,
    HealthStatusResponse,
    LivenessResponse,
)

router = APIRouter(prefix="/health", tags=["health"])

_OK_STATUSES = {ConnectionStatus.HEALTHY, ConnectionStatus.UNKNOWN}


@router.get("/live", response_model=LivenessResponse)
def live() -> LivenessResponse:
    return LivenessResponse()


@router.get("", response_model=HealthStatusResponse)
def health(db: DbSession, _: ViewerUser) -> HealthStatusResponse:
    settings = get_settings()
    started = time.perf_counter()
    connections: list[AirflowConnection] = []
    try:
        db.execute(text("SELECT 1"))
        database = DatabaseHealth(
            status="ok", latency_ms=int((time.perf_counter() - started) * 1000)
        )
        connections = list(
            db.scalars(
                select(AirflowConnection)
                .where(AirflowConnection.is_active.is_(True))
                .order_by(AirflowConnection.is_default.desc(), AirflowConnection.name)
            ).all()
        )
    except SQLAlchemyError:
        database = DatabaseHealth(status="error", message="Database query failed")

    airflow = [
        AirflowConnectionHealth(
            id=c.id,
            name=c.name,
            is_default=c.is_default,
            status=c.last_health_status,
            message=c.last_health_message,
            last_checked_at=c.last_checked_at,
        )
        for c in connections
    ]
    degraded = (
        database.status != "ok"
        or any(a.status not in _OK_STATUSES for a in airflow)
        or any(c.status_stale for c in connections)  # the connection monitor is not reporting
    )
    return HealthStatusResponse(
        status="degraded" if degraded else "ok",
        version=settings.VERSION,
        environment=settings.ENVIRONMENT,
        database=database,
        airflow=airflow,
    )
