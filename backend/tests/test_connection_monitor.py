"""Connection monitor: live DAG refresh, health transitions, liveness and the refresh endpoint."""

import uuid
from datetime import timedelta

import httpx
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.db.base import utcnow
from app.models.airflow import AirflowConnection, MonitoredDag
from app.models.audit_log import AuditLog
from app.orchestration.airflow import factory
from app.services import connection_monitor_service
from tests.fake_airflow import FakeAirflow

CONNS = "/api/v1/airflow/connections"
LIVE = {
    "name": "prod-airflow",
    "environment": "PROD",
    "kind": "LIVE",
    "base_url": "http://airflow.test:8080",
    "auth_type": "BASIC",
    "username": "admin",
    "secret": "admin",
}


class Switchable:
    """Fake Airflow that can be taken offline (connection refused)."""

    def __init__(self) -> None:
        self.server = FakeAirflow(major=3)
        self.down = False

    def handler(self, request: httpx.Request) -> httpx.Response:
        if self.down:
            raise httpx.ConnectError("Connection refused", request=request)
        return self.server.handler(request)


@pytest.fixture
def airflow(monkeypatch: pytest.MonkeyPatch) -> Switchable:
    fake = Switchable()
    monkeypatch.setattr(factory, "live_transport", httpx.MockTransport(fake.handler))
    return fake


@pytest.fixture
def conn_id(client: TestClient, admin_headers: dict) -> uuid.UUID:
    response = client.post(CONNS, json=LIVE, headers=admin_headers)
    assert response.status_code == 201, response.text
    return uuid.UUID(response.json()["id"])


def _conn(db: Session, conn_id: uuid.UUID) -> AirflowConnection:
    db.expire_all()
    conn = db.get(AirflowConnection, conn_id)
    assert conn is not None
    return conn


def _transitions(db: Session) -> list[dict]:
    return [
        a.details
        for a in db.scalars(
            select(AuditLog)
            .where(AuditLog.action == "airflow_conn.status_changed")
            .order_by(AuditLog.created_at)
        )
    ]


def test_cycle_refreshes_dags_and_marks_healthy(
    db: Session, airflow: Switchable, conn_id: uuid.UUID
) -> None:
    summary = connection_monitor_service.run_cycle(db)
    assert (summary.connections, summary.healthy, summary.failing) == (1, 1, 0)

    conn = _conn(db, conn_id)
    assert conn.last_health_status == "HEALTHY"
    assert conn.is_live and not conn.status_stale
    dags = db.scalars(select(MonitoredDag).where(MonitoredDag.connection_id == conn_id)).all()
    assert sorted(d.dag_id for d in dags) == ["etl_a", "etl_b"]

    # DAG removed in Airflow is marked not present on the next cycle.
    airflow.server.dags = airflow.server.dags[:1]
    connection_monitor_service.run_cycle(db)
    db.expire_all()
    present = {d.dag_id: d.is_present for d in db.scalars(select(MonitoredDag))}
    assert present == {"etl_a": True, "etl_b": False}


def test_outage_and_recovery_are_detected_and_audited_once(
    db: Session, airflow: Switchable, conn_id: uuid.UUID
) -> None:
    connection_monitor_service.run_cycle(db)
    airflow.down = True
    connection_monitor_service.run_cycle(db)
    connection_monitor_service.run_cycle(db)  # still down: no second transition record

    conn = _conn(db, conn_id)
    assert conn.last_health_status == "UNREACHABLE"
    assert not conn.is_live

    airflow.down = False
    connection_monitor_service.run_cycle(db)
    assert _conn(db, conn_id).is_live
    assert [(t["from"], t["to"]) for t in _transitions(db)] == [
        ("UNKNOWN", "HEALTHY"),
        ("HEALTHY", "UNREACHABLE"),
        ("UNREACHABLE", "HEALTHY"),
    ]


def test_healthy_but_stale_is_not_live(
    db: Session, airflow: Switchable, conn_id: uuid.UUID
) -> None:
    connection_monitor_service.run_cycle(db)
    conn = _conn(db, conn_id)
    settings = get_settings()
    too_old = (
        2 * settings.AIRFLOW_MONITOR_INTERVAL_SECONDS
        + settings.AIRFLOW_CONNECT_TIMEOUT_SECONDS
        + settings.AIRFLOW_READ_TIMEOUT_SECONDS
        + 1
    )
    conn.last_checked_at = utcnow() - timedelta(seconds=too_old)
    db.commit()

    conn = _conn(db, conn_id)
    assert conn.last_health_status == "HEALTHY"
    assert conn.status_stale and not conn.is_live


def test_inactive_connection_is_skipped(
    db: Session, airflow: Switchable, conn_id: uuid.UUID
) -> None:
    conn = _conn(db, conn_id)
    conn.is_active = False
    db.commit()
    assert connection_monitor_service.run_cycle(db).connections == 0
    assert not _conn(db, conn_id).is_live


def test_refresh_endpoint(
    client: TestClient,
    airflow: Switchable,
    conn_id: uuid.UUID,
    operator_headers: dict,
    viewer_headers: dict,
) -> None:
    url = f"{CONNS}/{conn_id}/refresh"
    assert client.post(url, headers=viewer_headers).status_code == 403

    body = client.post(url, headers=operator_headers).json()
    assert body["last_health_status"] == "HEALTHY" and body["is_live"] is True

    airflow.down = True
    body = client.post(url, headers=operator_headers).json()
    assert body["last_health_status"] == "UNREACHABLE" and body["is_live"] is False

    listed = client.get(CONNS, headers=viewer_headers).json()["items"][0]
    assert listed["is_live"] is False and listed["status_stale"] is False
