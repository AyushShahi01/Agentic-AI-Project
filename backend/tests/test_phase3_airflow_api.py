"""Phase 3b: Airflow connection/DAG endpoints, secrets handling, SSRF guard, health."""

import uuid

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.models.airflow import AirflowConnection, MonitoredDag
from app.models.audit_log import AuditLog
from app.orchestration.airflow import factory
from tests.fake_airflow import FakeAirflow

CONNS = "/api/v1/airflow/connections"
LIVE = {
    "name": "prod-airflow",
    "environment": "PROD",
    "kind": "LIVE",
    "base_url": "http://airflow.test:8080/api/v2/",
    "auth_type": "BASIC",
    "username": "admin",
    "secret": "admin",
}
MOCK = {"name": "mock", "kind": "MOCK", "base_url": "http://mock-airflow", "auth_type": "NONE"}


@pytest.fixture
def fake(monkeypatch: pytest.MonkeyPatch) -> FakeAirflow:
    server = FakeAirflow(major=3)
    monkeypatch.setattr(factory, "live_transport", server.transport())
    return server


def create(client: TestClient, headers: dict, body: dict) -> dict:
    response = client.post(CONNS, json=body, headers=headers)
    assert response.status_code == 201, response.text
    return response.json()


# ---------------------------------------------------------------------- CRUD & secrets


def test_create_connection_normalizes_url_and_hides_secret(
    client: TestClient, admin_headers: dict, db: Session
) -> None:
    body = create(client, admin_headers, {**LIVE, "secret": "s3cret-pass"})
    assert body["base_url"] == "http://airflow.test:8080"
    assert body["has_secret"] is True
    assert "secret" not in body and "encrypted_secret" not in body
    assert body["last_health_status"] == "UNKNOWN"

    stored = db.get(AirflowConnection, uuid.UUID(body["id"]))
    assert stored.encrypted_secret and "s3cret-pass" not in stored.encrypted_secret
    assert "s3cret-pass" not in client.get(CONNS, headers=admin_headers).text
    audit = db.scalars(select(AuditLog).where(AuditLog.action == "airflow_conn.create")).one()
    assert "s3cret-pass" not in str(audit.details)


def test_create_requires_credentials_for_basic(client: TestClient, admin_headers: dict) -> None:
    body = {**LIVE, "secret": None}
    assert client.post(CONNS, json=body, headers=admin_headers).status_code == 422


def test_duplicate_name_conflict(client: TestClient, admin_headers: dict) -> None:
    create(client, admin_headers, MOCK)
    response = client.post(CONNS, json={**MOCK, "name": "MOCK"}, headers=admin_headers)
    assert response.status_code == 409


def test_only_one_default(client: TestClient, admin_headers: dict) -> None:
    a = create(client, admin_headers, {**MOCK, "name": "a", "is_default": True})
    b = create(client, admin_headers, {**MOCK, "name": "b", "is_default": True})
    items = {c["id"]: c for c in client.get(CONNS, headers=admin_headers).json()["items"]}
    assert items[b["id"]]["is_default"] is True
    assert items[a["id"]]["is_default"] is False


def test_update_keeps_secret_when_omitted_and_resets_health(
    client: TestClient, admin_headers: dict, fake: FakeAirflow
) -> None:
    conn = create(client, admin_headers, LIVE)
    assert client.post(f"{CONNS}/{conn['id']}/test", headers=admin_headers).json()["ok"]

    renamed = client.patch(f"{CONNS}/{conn['id']}", json={"name": "renamed"}, headers=admin_headers)
    assert renamed.json()["has_secret"] is True
    assert renamed.json()["last_health_status"] == "HEALTHY"  # name change keeps health

    moved = client.patch(
        f"{CONNS}/{conn['id']}",
        json={"base_url": "http://airflow.test:9090"},
        headers=admin_headers,
    ).json()
    assert moved["last_health_status"] == "UNKNOWN"
    assert moved["api_version"] is None


def test_delete_connection_cascades_dags(
    client: TestClient, admin_headers: dict, db: Session
) -> None:
    conn = create(client, admin_headers, MOCK)
    client.post(f"{CONNS}/{conn['id']}/sync-dags", headers=admin_headers)
    assert client.delete(f"{CONNS}/{conn['id']}", headers=admin_headers).status_code == 204
    db.expire_all()
    assert db.scalars(select(MonitoredDag)).first() is None


# ---------------------------------------------------------------------- RBAC


def test_operator_cannot_create_connection(client: TestClient, operator_headers: dict) -> None:
    assert client.post(CONNS, json=MOCK, headers=operator_headers).status_code == 403


def test_viewer_cannot_test_or_sync(
    client: TestClient, admin_headers: dict, viewer_headers: dict
) -> None:
    conn = create(client, admin_headers, MOCK)
    assert client.post(f"{CONNS}/{conn['id']}/test", headers=viewer_headers).status_code == 403
    assert client.post(f"{CONNS}/{conn['id']}/sync-dags", headers=viewer_headers).status_code == 403
    unsaved = client.post("/api/v1/airflow/test-connection", json=MOCK, headers=viewer_headers)
    assert unsaved.status_code == 403
    assert client.get(CONNS, headers=viewer_headers).status_code == 200


# ---------------------------------------------------------------------- testing connections


def test_unsaved_connection_test(
    client: TestClient, operator_headers: dict, fake: FakeAirflow, db: Session
) -> None:
    body = {k: v for k, v in LIVE.items() if k not in ("name", "environment")}
    result = client.post("/api/v1/airflow/test-connection", json=body, headers=operator_headers)
    assert result.status_code == 200
    data = result.json()
    assert data["status"] == "HEALTHY" and data["ok"] is True
    assert data["airflow_version"] == "3.1.0" and data["api_version"] == "v2"
    assert data["latency_ms"] is not None
    assert "airflow_conn.test" in [a.action for a in db.scalars(select(AuditLog))]


def test_saved_connection_test_persists_health(
    client: TestClient, admin_headers: dict, operator_headers: dict, fake: FakeAirflow
) -> None:
    conn = create(client, admin_headers, LIVE)
    data = client.post(f"{CONNS}/{conn['id']}/test", headers=operator_headers).json()
    assert data["status"] == "HEALTHY"
    stored = client.get(f"{CONNS}/{conn['id']}", headers=operator_headers).json()
    assert stored["last_health_status"] == "HEALTHY"
    assert stored["airflow_version"] == "3.1.0"
    assert stored["last_checked_at"] is not None


def test_saved_connection_bad_credentials(
    client: TestClient, admin_headers: dict, fake: FakeAirflow
) -> None:
    conn = create(client, admin_headers, {**LIVE, "secret": "wrong"})
    data = client.post(f"{CONNS}/{conn['id']}/test", headers=admin_headers).json()
    assert data["status"] == "UNAUTHORIZED" and data["ok"] is False


def test_airflow2_connection(
    client: TestClient, admin_headers: dict, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(factory, "live_transport", FakeAirflow(major=2).transport())
    conn = create(client, admin_headers, LIVE)
    data = client.post(f"{CONNS}/{conn['id']}/test", headers=admin_headers).json()
    assert data["api_version"] == "v1" and data["airflow_version"] == "2.1.0"


# ---------------------------------------------------------------------- SSRF guard & mock guard


def test_allowed_hosts_enforced(
    client: TestClient, admin_headers: dict, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(get_settings(), "AIRFLOW_ALLOWED_HOSTS", ["airflow.internal"])
    blocked = client.post(
        "/api/v1/airflow/test-connection",
        json={**LIVE, "base_url": "http://169.254.169.254"},
        headers=admin_headers,
    )
    assert blocked.status_code == 400
    assert blocked.json()["error"]["code"] == "host_not_allowed"
    assert client.post(CONNS, json=LIVE, headers=admin_headers).status_code == 400


def test_non_http_scheme_rejected(client: TestClient, admin_headers: dict) -> None:
    response = client.post(
        "/api/v1/airflow/test-connection",
        json={**LIVE, "base_url": "file:///etc/passwd"},
        headers=admin_headers,
    )
    assert response.status_code == 422


def test_mock_blocked_in_production(
    client: TestClient, admin_headers: dict, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = create(client, admin_headers, MOCK)
    monkeypatch.setattr(get_settings(), "ENVIRONMENT", "production")
    assert client.post(CONNS, json={**MOCK, "name": "m2"}, headers=admin_headers).status_code == 400
    assert client.post(f"{CONNS}/{conn['id']}/test", headers=admin_headers).status_code == 400


# ---------------------------------------------------------------------- DAG sync & monitoring


def test_sync_upserts_and_marks_missing_preserving_monitoring(
    client: TestClient, admin_headers: dict, operator_headers: dict, fake: FakeAirflow
) -> None:
    conn = create(client, admin_headers, LIVE)
    first = client.post(f"{CONNS}/{conn['id']}/sync-dags", headers=operator_headers).json()
    assert (first["total"], first["created"], first["updated"], first["missing"]) == (2, 2, 0, 0)

    dags = client.get(f"{CONNS}/{conn['id']}/dags", headers=operator_headers).json()["items"]
    etl_a = next(d for d in dags if d["dag_id"] == "etl_a")
    assert etl_a["is_monitored"] is False
    toggled = client.patch(
        f"/api/v1/airflow/dags/{etl_a['id']}", json={"is_monitored": True}, headers=operator_headers
    )
    assert toggled.json()["is_monitored"] is True

    fake.dags = [d for d in fake.dags if d["dag_id"] == "etl_a"] + [{"dag_id": "etl_c", "tags": []}]
    second = client.post(f"{CONNS}/{conn['id']}/sync-dags", headers=operator_headers).json()
    assert (second["created"], second["updated"], second["missing"]) == (1, 1, 1)

    dags = {
        d["dag_id"]: d
        for d in client.get(f"{CONNS}/{conn['id']}/dags", headers=operator_headers).json()["items"]
    }
    assert dags["etl_a"]["is_monitored"] is True  # preserved across sync
    assert dags["etl_b"]["is_present"] is False
    assert dags["etl_c"]["is_present"] is True


def test_dag_filters(client: TestClient, admin_headers: dict) -> None:
    conn = create(client, admin_headers, MOCK)
    client.post(f"{CONNS}/{conn['id']}/sync-dags", headers=admin_headers)
    url = f"{CONNS}/{conn['id']}/dags"
    assert client.get(f"{url}?tag=tier-1", headers=admin_headers).json()["total"] == 2
    assert (
        client.get(f"{url}?search=orders", headers=admin_headers).json()["total"] == 2
    )  # + partner_api_sync
    assert client.get(f"{url}?monitored=true", headers=admin_headers).json()["total"] == 0
    page = client.get(f"{url}?limit=1&offset=1", headers=admin_headers).json()
    assert page["total"] == 5 and len(page["items"]) == 1


def test_sync_failure_returns_502_and_records_health(
    client: TestClient, admin_headers: dict, fake: FakeAirflow
) -> None:
    conn = create(client, admin_headers, {**LIVE, "secret": "wrong"})
    response = client.post(f"{CONNS}/{conn['id']}/sync-dags", headers=admin_headers)
    assert response.status_code == 502
    assert response.json()["error"]["details"]["status"] == "UNAUTHORIZED"
    stored = client.get(f"{CONNS}/{conn['id']}", headers=admin_headers).json()
    assert stored["last_health_status"] == "UNAUTHORIZED"


def test_monitor_toggle_audited(client: TestClient, admin_headers: dict, db: Session) -> None:
    conn = create(client, admin_headers, MOCK)
    client.post(f"{CONNS}/{conn['id']}/sync-dags", headers=admin_headers)
    dag = client.get(f"{CONNS}/{conn['id']}/dags", headers=admin_headers).json()["items"][0]
    client.patch(
        f"/api/v1/airflow/dags/{dag['id']}", json={"is_monitored": True}, headers=admin_headers
    )
    actions = [a.action for a in db.scalars(select(AuditLog))]
    assert "airflow_dags.sync" in actions and "monitored_dag.toggle" in actions


# ---------------------------------------------------------------------- health


def test_liveness_is_public(client: TestClient) -> None:
    assert client.get("/api/v1/health/live").json() == {"status": "ok"}


def test_detailed_health_requires_auth_and_reports_connections(
    client: TestClient, admin_headers: dict, viewer_headers: dict
) -> None:
    assert client.get("/api/v1/health").status_code == 401
    conn = create(client, admin_headers, {**MOCK, "base_url": "http://mock?simulate=UNREACHABLE"})
    health = client.get("/api/v1/health", headers=viewer_headers).json()
    assert health["database"]["status"] == "ok"
    assert health["status"] == "ok"  # untested connections are UNKNOWN, not degraded
    client.post(f"{CONNS}/{conn['id']}/test", headers=admin_headers)
    health = client.get("/api/v1/health", headers=viewer_headers).json()
    assert health["status"] == "degraded"
    assert health["airflow"][0]["status"] == "UNREACHABLE"


def test_error_shape_for_unknown_route(client: TestClient) -> None:
    body = client.get("/api/v1/nope").json()
    assert body["error"]["code"] == "http_404"
