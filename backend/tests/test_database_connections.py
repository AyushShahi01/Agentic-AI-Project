"""Database connections in the catalog: CRUD, RBAC, probing, monitor cycle and health."""

import uuid

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, select
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session

from app.connectors import database as db_connector
from app.core.config import get_settings
from app.models.audit_log import AuditLog
from app.models.database_connection import DatabaseConnection
from app.orchestration.airflow.base import ConnectionStatus
from app.services import connection_monitor_service

CONNS = "/api/v1/databases/connections"
PG = {
    "name": "warehouse",
    "environment": "PROD",
    "engine": "POSTGRESQL",
    "host": "db.test",
    "database": "analytics",
    "username": "reader",
    "secret": "s3cret-pw",
    "ssl_mode": "require",
}


class FakeServer:
    """Stands in for the database server: SQLite when up, a driver error when not."""

    def __init__(self) -> None:
        self.error: str | None = None
        self.urls: list = []

    def factory(self, url, connect_args):
        self.urls.append(url)
        if self.error:
            raise OperationalError("connect", {}, Exception(self.error))
        return create_engine("sqlite://")


@pytest.fixture
def server(monkeypatch: pytest.MonkeyPatch) -> FakeServer:
    fake = FakeServer()
    monkeypatch.setattr(db_connector, "engine_factory", fake.factory)
    return fake


@pytest.fixture
def conn_id(client: TestClient, admin_headers: dict) -> uuid.UUID:
    response = client.post(CONNS, json=PG, headers=admin_headers)
    assert response.status_code == 201, response.text
    return uuid.UUID(response.json()["id"])


def _conn(db: Session, conn_id: uuid.UUID) -> DatabaseConnection:
    db.expire_all()
    conn = db.get(DatabaseConnection, conn_id)
    assert conn is not None
    return conn


# ---------------------------------------------------------------------- CRUD & RBAC


def test_create_defaults_port_and_never_returns_secret(
    client: TestClient, admin_headers: dict, db: Session, conn_id: uuid.UUID
) -> None:
    body = client.get(f"{CONNS}/{conn_id}", headers=admin_headers).json()
    assert body["port"] == 5432
    assert body["has_secret"] is True
    assert body["last_health_status"] == "UNKNOWN"
    assert "secret" not in body and "encrypted_secret" not in body
    assert "s3cret-pw" not in client.get(CONNS, headers=admin_headers).text
    assert _conn(db, conn_id).encrypted_secret != "s3cret-pw"
    audit = db.scalars(select(AuditLog).where(AuditLog.action == "db_conn.create")).one()
    assert "s3cret-pw" not in str(audit.details)


def test_mysql_drops_ssl_mode(client: TestClient, admin_headers: dict) -> None:
    body = {**PG, "name": "orders", "engine": "MYSQL"}
    created = client.post(CONNS, json=body, headers=admin_headers).json()
    assert created["port"] == 3306
    assert created["ssl_mode"] is None


def test_duplicate_name_conflicts(
    client: TestClient, admin_headers: dict, conn_id: uuid.UUID
) -> None:
    response = client.post(CONNS, json={**PG, "name": "WAREHOUSE"}, headers=admin_headers)
    assert response.status_code == 409


def test_rbac(
    client: TestClient,
    admin_headers: dict,
    operator_headers: dict,
    viewer_headers: dict,
    server: FakeServer,
    conn_id: uuid.UUID,
) -> None:
    assert client.get(CONNS, headers=viewer_headers).status_code == 200
    assert client.post(CONNS, json={**PG, "name": "x"}, headers=operator_headers).status_code == 403
    assert client.post(f"{CONNS}/{conn_id}/test", headers=viewer_headers).status_code == 403
    assert client.post(f"{CONNS}/{conn_id}/test", headers=operator_headers).status_code == 200
    assert client.delete(f"{CONNS}/{conn_id}", headers=operator_headers).status_code == 403
    assert client.delete(f"{CONNS}/{conn_id}", headers=admin_headers).status_code == 204
    assert client.get(f"{CONNS}/{conn_id}", headers=admin_headers).status_code == 404


def test_host_allowlist(
    client: TestClient, admin_headers: dict, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(get_settings(), "DB_CONNECTION_ALLOWED_HOSTS", ["db.internal"])
    response = client.post(CONNS, json=PG, headers=admin_headers)
    assert response.status_code == 400
    assert response.json()["error"]["code"] == "host_not_allowed"


def test_update_target_resets_health(
    client: TestClient, admin_headers: dict, db: Session, server: FakeServer, conn_id: uuid.UUID
) -> None:
    client.post(f"{CONNS}/{conn_id}/test", headers=admin_headers)
    assert _conn(db, conn_id).last_health_status == ConnectionStatus.HEALTHY

    renamed = client.patch(f"{CONNS}/{conn_id}", json={"name": "dwh"}, headers=admin_headers)
    assert renamed.json()["last_health_status"] == "HEALTHY"  # name only: health kept

    moved = client.patch(f"{CONNS}/{conn_id}", json={"host": "db2.test"}, headers=admin_headers)
    assert moved.json()["last_health_status"] == "UNKNOWN"
    assert moved.json()["has_secret"] is True  # secret untouched when omitted

    cleared = client.patch(f"{CONNS}/{conn_id}", json={"secret": None}, headers=admin_headers)
    assert cleared.json()["has_secret"] is False


# ---------------------------------------------------------------------- probing


def test_saved_test_records_health_and_passes_target(
    client: TestClient, admin_headers: dict, db: Session, server: FakeServer, conn_id: uuid.UUID
) -> None:
    result = client.post(f"{CONNS}/{conn_id}/test", headers=admin_headers).json()
    assert result["ok"] is True
    assert result["status"] == "HEALTHY"
    assert result["server_version"]  # SQLite reports one

    url = server.urls[-1]
    assert url.drivername == "postgresql+psycopg"
    assert (url.host, url.port, url.database) == ("db.test", 5432, "analytics")
    assert url.password == "s3cret-pw"
    assert url.query["sslmode"] == "require"

    conn = _conn(db, conn_id)
    assert conn.last_checked_at is not None
    assert conn.server_version == result["server_version"]


def test_unsaved_test(client: TestClient, operator_headers: dict, server: FakeServer) -> None:
    body = {k: v for k, v in PG.items() if k not in ("name", "environment")}
    result = client.post("/api/v1/databases/test-connection", json=body, headers=operator_headers)
    assert result.status_code == 200, result.text
    assert result.json()["ok"] is True


@pytest.mark.parametrize(
    ("error", "status"),
    [
        ('FATAL:  password authentication failed for user "reader"', "UNAUTHORIZED"),
        ("(1045, \"Access denied for user 'reader'@'10.0.0.1'\")", "UNAUTHORIZED"),
        ("FATAL:  permission denied for database analytics", "FORBIDDEN"),
        ("SSL error: certificate verify failed", "TLS_ERROR"),
        ('FATAL:  database "analytics" does not exist', "INVALID_ENDPOINT"),
        ('connection to server at "db.test" port 5432 failed: Connection refused', "UNREACHABLE"),
    ],
)
def test_error_mapping(
    client: TestClient,
    admin_headers: dict,
    server: FakeServer,
    conn_id: uuid.UUID,
    error: str,
    status: str,
) -> None:
    server.error = error
    result = client.post(f"{CONNS}/{conn_id}/test", headers=admin_headers).json()
    assert result["ok"] is False
    assert result["status"] == status
    assert "s3cret-pw" not in result["message"]


# ---------------------------------------------------------------------- monitor & health


def test_monitor_cycle_checks_databases(
    client: TestClient, admin_headers: dict, db: Session, server: FakeServer, conn_id: uuid.UUID
) -> None:
    summary = connection_monitor_service.run_cycle(db)
    assert (summary.databases_healthy, summary.databases_failing) == (1, 0)
    assert _conn(db, conn_id).last_health_status == ConnectionStatus.HEALTHY

    health = client.get("/api/v1/health", headers=admin_headers).json()
    assert health["status"] == "ok"
    assert health["databases"][0]["name"] == "warehouse"

    server.error = "Connection refused"
    summary = connection_monitor_service.run_cycle(db)
    assert (summary.databases_healthy, summary.databases_failing) == (0, 1)
    assert {"connection": "warehouse", "from": "HEALTHY", "to": "UNREACHABLE"} in [
        {k: str(v) for k, v in t.items()} for t in summary.transitions
    ]
    assert db.scalars(select(AuditLog).where(AuditLog.action == "db_conn.status_changed")).all()

    health = client.get("/api/v1/health", headers=admin_headers).json()
    assert health["status"] == "degraded"
    assert health["databases"][0]["status"] == "UNREACHABLE"


def test_monitor_skips_inactive(
    client: TestClient, admin_headers: dict, db: Session, server: FakeServer, conn_id: uuid.UUID
) -> None:
    client.patch(f"{CONNS}/{conn_id}", json={"is_active": False}, headers=admin_headers)
    summary = connection_monitor_service.run_cycle(db)
    assert (summary.databases_healthy, summary.databases_failing) == (0, 0)
    assert server.urls == []


def test_real_driver_refused_port_is_unreachable() -> None:
    """No fake: psycopg against a closed local port fails fast and maps to UNREACHABLE."""
    target = db_connector.DbTarget(
        engine="POSTGRESQL", host="127.0.0.1", port=1, database="x", username="x", secret="x"
    )
    result = db_connector.probe(target, timeout_seconds=3)
    assert result.status == ConnectionStatus.UNREACHABLE
    assert result.message.startswith("Could not connect")
