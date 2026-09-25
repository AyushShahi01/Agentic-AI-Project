"""Phase 1: configuration safety, migrations and database constraints."""

import os
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from pydantic import ValidationError
from sqlalchemy import create_engine, inspect
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.core.config import BACKEND_DIR, Settings
from app.core.crypto import decrypt_secret, encrypt_secret
from app.models.airflow import (
    AirflowConnection,
    ConnectionKind,
    DeploymentEnvironment,
    MonitoredDag,
)
from app.models.audit_log import AuditLog
from app.models.user import User, UserRole
from app.orchestration.airflow.base import AuthType
from app.services import audit_service

SAFE_PROD = {
    "ENVIRONMENT": "production",
    "DEBUG": False,
    "DATABASE_URL": "postgresql+psycopg://u:p@db/agentic",
    "JWT_SECRET_KEY": "x" * 64,
    "ENCRYPTION_KEY": "5ce7pnzbhWn6iVmlv3zGtFnEAH5OhEpOSzb_Y8f4Ehk=",
    "ADMIN_PASSWORD": "a-real-admin-password",
}


# ---------------------------------------------------------------------- config


def test_production_config_accepts_safe_values() -> None:
    Settings(_env_file=None, **SAFE_PROD)


@pytest.mark.parametrize("field", ["JWT_SECRET_KEY", "ENCRYPTION_KEY", "ADMIN_PASSWORD"])
def test_production_rejects_placeholder_secrets(field: str) -> None:
    with pytest.raises(ValidationError, match=field):
        Settings(_env_file=None, **{**SAFE_PROD, field: "CHANGE_ME"})


@pytest.mark.parametrize(
    ("override", "message"),
    [
        ({"DEBUG": True}, "DEBUG"),
        ({"DATABASE_URL": "sqlite:///./x.db"}, "SQLite"),
        ({"ENVIRONMENT": "staging", "DEBUG": True}, "DEBUG"),
    ],
)
def test_non_development_rejects_unsafe_settings(override: dict, message: str) -> None:
    with pytest.raises(ValidationError, match=message):
        Settings(_env_file=None, **{**SAFE_PROD, **override})


def test_development_allows_placeholders_with_dev_keys() -> None:
    s = Settings(
        _env_file=None,
        ENVIRONMENT="development",
        JWT_SECRET_KEY="CHANGE_ME",
        ENCRYPTION_KEY="CHANGE_ME",
    )
    assert s.jwt_secret != "CHANGE_ME"
    assert len(s.encryption_keys) == 1


def test_secret_encryption_round_trip() -> None:
    token = encrypt_secret("s3cr3t")
    assert "s3cr3t" not in token
    assert decrypt_secret(token) == "s3cr3t"


# ---------------------------------------------------------------------- migrations


def _alembic_config(url: str) -> Config:
    cfg = Config(str(BACKEND_DIR / "alembic.ini"))
    cfg.set_main_option("script_location", str(BACKEND_DIR / "alembic"))
    cfg.set_main_option("sqlalchemy.url", url)
    cfg.attributes["configure_logger"] = False
    return cfg


EXPECTED_TABLES = {
    "users",
    "refresh_tokens",
    "airflow_connections",
    "monitored_dags",
    "audit_logs",
    "incidents",
    "automation_workflows",
    "workflow_runs",
    "workflow_steps",
    "approvals",
    "notifications",
}


def _round_trip(url: str) -> None:
    cfg = _alembic_config(url)
    engine = create_engine(url)
    try:
        command.upgrade(cfg, "head")
        assert EXPECTED_TABLES <= set(inspect(engine).get_table_names())
        command.downgrade(cfg, "base")
        assert not EXPECTED_TABLES & set(inspect(engine).get_table_names())
        command.upgrade(cfg, "head")
        assert EXPECTED_TABLES <= set(inspect(engine).get_table_names())
    finally:
        engine.dispose()


def test_migrations_round_trip_sqlite(tmp_path: Path) -> None:
    _round_trip(f"sqlite:///{(tmp_path / 'migrations.db').as_posix()}")


@pytest.mark.skipif(not os.getenv("TEST_POSTGRES_URL"), reason="TEST_POSTGRES_URL not set")
def test_migrations_round_trip_postgres() -> None:
    url = os.environ["TEST_POSTGRES_URL"]
    try:
        _round_trip(url)
    finally:
        command.downgrade(_alembic_config(url), "base")


# ---------------------------------------------------------------------- constraints


def _conn(name: str, **kwargs: object) -> AirflowConnection:
    return AirflowConnection(
        name=name,
        environment=DeploymentEnvironment.DEV,
        kind=ConnectionKind.MOCK,
        base_url="http://mock",
        auth_type=AuthType.NONE,
        **kwargs,
    )


def _user(email: str) -> User:
    return User(email=email, full_name="x", hashed_password="x", role=UserRole.VIEWER)


def test_unique_user_email(db: Session) -> None:
    db.add_all([_user("a@example.com"), _user("a@example.com")])
    with pytest.raises(IntegrityError):
        db.commit()


def test_unique_connection_name(db: Session) -> None:
    db.add_all([_conn("same"), _conn("same")])
    with pytest.raises(IntegrityError):
        db.commit()


def test_only_one_default_connection(db: Session) -> None:
    db.add_all([_conn("a", is_default=True), _conn("b", is_default=False), _conn("c")])
    db.commit()
    db.add(_conn("d", is_default=True))
    with pytest.raises(IntegrityError):
        db.commit()


def test_unique_dag_per_connection(db: Session) -> None:
    conn = _conn("a")
    db.add(conn)
    db.flush()
    db.add_all(
        [
            MonitoredDag(connection_id=conn.id, dag_id="etl"),
            MonitoredDag(connection_id=conn.id, dag_id="etl"),
        ]
    )
    with pytest.raises(IntegrityError):
        db.commit()


def test_audit_log_is_append_only(db: Session) -> None:
    entry = audit_service.record(db, action="test.action", entity_type="thing")
    db.commit()
    entry.action = "changed"
    with pytest.raises(RuntimeError, match="append-only"):
        db.commit()
    db.rollback()
    db.delete(db.get(AuditLog, entry.id))
    with pytest.raises(RuntimeError, match="append-only"):
        db.commit()


def test_audit_details_redact_secrets(db: Session) -> None:
    entry = audit_service.record(
        db,
        action="test.action",
        entity_type="thing",
        details={"password": "hunter2", "nested": {"token": "abc"}, "secret_changed": True},
    )
    db.commit()
    assert entry.details == {
        "password": "***",
        "nested": {"token": "***"},
        "secret_changed": True,
    }


def test_timestamps_are_timezone_aware(db: Session) -> None:
    user = _user("tz@example.com")
    db.add(user)
    db.commit()
    db.expire_all()
    assert db.get(User, user.id).created_at.tzinfo is not None
