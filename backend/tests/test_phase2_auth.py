"""Phase 2: authentication, tokens, RBAC, user management, bootstrap."""

import uuid

import jwt
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.core.security import create_access_token, create_refresh_token, decode_token
from app.db.base import utcnow
from app.db.init_db import bootstrap_admin
from app.models.audit_log import AuditLog
from app.models.user import RefreshToken, User, UserRole
from tests.conftest import PASSWORD, login, make_user

LOGIN = "/api/v1/auth/login"
REFRESH = "/api/v1/auth/refresh"
LOGOUT = "/api/v1/auth/logout"
ME = "/api/v1/auth/me"


def actions(db: Session) -> list[str]:
    db.expire_all()
    return list(db.scalars(select(AuditLog.action).order_by(AuditLog.created_at)).all())


# ---------------------------------------------------------------------- login


def test_login_success_sets_httponly_refresh_cookie(
    client: TestClient, admin: User, db: Session
) -> None:
    response = client.post(LOGIN, json={"email": "ADMIN@example.com", "password": PASSWORD})
    assert response.status_code == 200
    body = response.json()
    assert body["token_type"] == "bearer"
    assert body["user"]["email"] == "admin@example.com"
    assert "hashed_password" not in body["user"]
    cookie = response.headers["set-cookie"]
    assert "refresh_token=" in cookie
    assert "HttpOnly" in cookie
    assert "Path=/api/v1/auth" in cookie
    assert "samesite=strict" in cookie.lower()
    assert "auth.login_success" in actions(db)


@pytest.mark.parametrize(
    ("email", "password"),
    [("admin@example.com", "wrong-password"), ("nobody@example.com", PASSWORD)],
)
def test_login_failure_is_generic_and_audited(
    client: TestClient, admin: User, db: Session, email: str, password: str
) -> None:
    response = client.post(LOGIN, json={"email": email, "password": password})
    assert response.status_code == 401
    assert response.json()["error"]["message"] == "Incorrect email or password"
    assert "auth.login_failed" in actions(db)


def test_login_rejects_inactive_user(client: TestClient, db: Session) -> None:
    user = make_user(db, UserRole.VIEWER, "gone@example.com")
    user.is_active = False
    db.commit()
    assert client.post(LOGIN, json={"email": user.email, "password": PASSWORD}).status_code == 401


def test_login_lockout_after_max_failures(client: TestClient, admin: User) -> None:
    for _ in range(get_settings().LOGIN_MAX_ATTEMPTS):
        client.post(LOGIN, json={"email": admin.email, "password": "wrong-password"})
    response = client.post(LOGIN, json={"email": admin.email, "password": PASSWORD})
    assert response.status_code == 429
    assert response.json()["error"]["code"] == "rate_limited"
    assert "Retry-After" in response.headers


# ---------------------------------------------------------------------- token types


def test_refresh_token_cannot_be_used_as_access_token(client: TestClient, admin: User) -> None:
    refresh = create_refresh_token(admin.id, uuid.uuid4(), utcnow().replace(year=2100))
    response = client.get(ME, headers={"Authorization": f"Bearer {refresh}"})
    assert response.status_code == 401


def test_access_token_cannot_be_used_as_refresh_token(admin: User) -> None:
    access, _ = create_access_token(admin.id, admin.role)
    with pytest.raises(jwt.InvalidTokenError):
        decode_token(access, "refresh")


def test_missing_or_garbage_token_rejected(client: TestClient) -> None:
    assert client.get(ME).status_code == 401
    assert client.get(ME, headers={"Authorization": "Bearer garbage"}).status_code == 401


# ---------------------------------------------------------------------- refresh


def test_refresh_rotates_token(client: TestClient, admin: User, db: Session) -> None:
    login(client, admin.email)
    first_cookie = client.cookies.get("refresh_token")
    response = client.post(REFRESH)
    assert response.status_code == 200
    assert response.json()["access_token"]
    assert client.cookies.get("refresh_token") != first_cookie

    tokens = db.scalars(select(RefreshToken).where(RefreshToken.user_id == admin.id)).all()
    assert len(tokens) == 2
    old = next(t for t in tokens if t.revoked_at is not None)
    assert old.replaced_by is not None


def test_refresh_reuse_revokes_whole_chain(client: TestClient, admin: User, db: Session) -> None:
    login(client, admin.email)
    stolen = client.cookies.get("refresh_token")
    assert client.post(REFRESH).status_code == 200  # legit rotation
    current = client.cookies.get("refresh_token")

    client.cookies.set("refresh_token", stolen, path="/api/v1/auth")
    assert client.post(REFRESH).status_code == 401  # reuse detected

    client.cookies.set("refresh_token", current, path="/api/v1/auth")
    assert client.post(REFRESH).status_code == 401  # the legit chain is revoked too
    assert "auth.refresh_reuse_detected" in actions(db)


def test_refresh_without_cookie_rejected(client: TestClient) -> None:
    assert client.post(REFRESH).status_code == 401


def test_logout_revokes_refresh_token(client: TestClient, admin: User, db: Session) -> None:
    login(client, admin.email)
    token = client.cookies.get("refresh_token")
    response = client.post(LOGOUT)
    assert response.status_code == 204
    client.cookies.set("refresh_token", token, path="/api/v1/auth")
    assert client.post(REFRESH).status_code == 401
    assert "auth.logout" in actions(db)


# ---------------------------------------------------------------------- DB-backed identity


def test_deactivated_user_rejected_immediately(
    client: TestClient, admin_headers: dict, viewer: User, viewer_headers: dict
) -> None:
    assert client.get(ME, headers=viewer_headers).status_code == 200
    r = client.patch(f"/api/v1/users/{viewer.id}", json={"is_active": False}, headers=admin_headers)
    assert r.status_code == 200
    assert client.get(ME, headers=viewer_headers).status_code == 401


def test_role_change_applies_on_next_request(
    client: TestClient, admin_headers: dict, operator: User, operator_headers: dict
) -> None:
    assert (
        client.post(
            "/api/v1/airflow/connections/" + str(uuid.uuid4()) + "/test", headers=operator_headers
        ).status_code
        == 404
    )  # allowed, just not found
    client.patch(f"/api/v1/users/{operator.id}", json={"role": "VIEWER"}, headers=admin_headers)
    response = client.post(
        f"/api/v1/airflow/connections/{uuid.uuid4()}/test", headers=operator_headers
    )
    assert response.status_code == 403


def test_deactivation_revokes_refresh_tokens(
    client: TestClient, admin_headers: dict, viewer: User, db: Session
) -> None:
    other = TestClient(client.app)
    login(other, viewer.email)
    client.patch(f"/api/v1/users/{viewer.id}", json={"is_active": False}, headers=admin_headers)
    assert other.post(REFRESH).status_code == 401


# ---------------------------------------------------------------------- user management


def test_admin_creates_and_lists_users(
    client: TestClient, admin_headers: dict, db: Session
) -> None:
    response = client.post(
        "/api/v1/users",
        json={
            "email": "New@Example.com",
            "full_name": "New",
            "password": "x" * 12,
            "role": "OPERATOR",
        },
        headers=admin_headers,
    )
    assert response.status_code == 201
    assert response.json()["email"] == "new@example.com"

    dup = client.post(
        "/api/v1/users",
        json={"email": "new@example.com", "full_name": "Dup", "password": "x" * 12},
        headers=admin_headers,
    )
    assert dup.status_code == 409

    listing = client.get("/api/v1/users?role=OPERATOR", headers=admin_headers).json()
    assert listing["total"] == 1
    assert listing["items"][0]["role"] == "OPERATOR"
    assert "user.create" in actions(db)


def test_short_password_rejected(client: TestClient, admin_headers: dict) -> None:
    response = client.post(
        "/api/v1/users",
        json={"email": "s@example.com", "full_name": "S", "password": "short"},
        headers=admin_headers,
    )
    assert response.status_code == 422
    assert response.json()["error"]["code"] == "validation_error"


@pytest.mark.parametrize("headers_fixture", ["operator_headers", "viewer_headers"])
def test_non_admin_cannot_manage_users(
    client: TestClient, headers_fixture: str, request: pytest.FixtureRequest
) -> None:
    headers = request.getfixturevalue(headers_fixture)
    assert client.get("/api/v1/users", headers=headers).status_code == 403
    response = client.post(
        "/api/v1/users",
        json={"email": "x@example.com", "full_name": "X", "password": "x" * 12},
        headers=headers,
    )
    assert response.status_code == 403
    assert response.json()["error"]["code"] == "forbidden"


@pytest.mark.parametrize("change", [{"role": "OPERATOR"}, {"is_active": False}])
def test_last_admin_cannot_be_demoted_or_deactivated(
    client: TestClient, admin: User, admin_headers: dict, change: dict
) -> None:
    response = client.patch(f"/api/v1/users/{admin.id}", json=change, headers=admin_headers)
    assert response.status_code == 409


def test_admin_can_be_demoted_when_another_admin_exists(
    client: TestClient, admin: User, admin_headers: dict, db: Session
) -> None:
    make_user(db, UserRole.ADMIN, "admin2@example.com")
    response = client.patch(
        f"/api/v1/users/{admin.id}", json={"role": "OPERATOR"}, headers=admin_headers
    )
    assert response.status_code == 200


def test_user_update_is_audited_without_password(
    client: TestClient, admin_headers: dict, viewer: User, db: Session
) -> None:
    client.patch(
        f"/api/v1/users/{viewer.id}",
        json={"full_name": "Renamed", "password": "brand-new-password"},
        headers=admin_headers,
    )
    entry = db.scalars(select(AuditLog).where(AuditLog.action == "user.update")).one()
    assert entry.details["password_changed"] is True
    assert "brand-new-password" not in str(entry.details)


# ---------------------------------------------------------------------- bootstrap


def test_bootstrap_creates_admin_once(db: Session, monkeypatch: pytest.MonkeyPatch) -> None:
    settings = get_settings()
    monkeypatch.setattr(settings, "ADMIN_PASSWORD", "bootstrap-password-123")
    admin = bootstrap_admin(db)
    assert admin is not None and admin.role == UserRole.ADMIN
    assert bootstrap_admin(db) is None
    assert "system.bootstrap_admin" in actions(db)


def test_bootstrap_skipped_with_placeholder_password(db: Session) -> None:
    assert bootstrap_admin(db) is None
    assert db.scalars(select(User)).first() is None
