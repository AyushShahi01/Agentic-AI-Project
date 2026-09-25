"""Plan 4 / Phase 1: the automation engine runs on its own leased loop."""

from datetime import timedelta

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from app.automation.types import RunStatus
from app.main import create_app, run_detection_cycle
from app.models.airflow import DeploymentEnvironment
from app.models.automation import WorkflowRun
from app.orchestration.airflow import mock as mock_module
from app.services import detection_service
from tests.test_plan2_engine import Clock, detect, enable, make_env


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> Clock:
    c = Clock(600, 4)  # partner_api_sync just failed
    monkeypatch.setattr(mock_module, "clock", c)
    return c


def test_leases_are_exclusive_per_name(db: Session) -> None:
    now = detection_service.utcnow()
    lease = detection_service.try_acquire_lease
    assert lease(db, "a", 60, now, name="automation")
    assert not lease(db, "b", 60, now, name="automation")
    assert lease(db, "b", 60, now)  # the detector lease is separate
    assert lease(db, "b", 60, now + timedelta(seconds=61), name="automation")  # expired
    assert detection_service.get_lease(db, "automation").holder == "b"


def test_scheduled_detection_does_not_tick_but_manual_does(db: Session, clock: Clock) -> None:
    make_env(db, DeploymentEnvironment.DEV, "partner_api_sync")
    enable(db, "retry-transient")
    summary = run_detection_cycle(db, trigger="schedule")
    assert summary.automation_queued == 1 and summary.automation is None
    run = db.scalars(select(WorkflowRun)).one()
    assert run.status == RunStatus.PENDING

    manual = run_detection_cycle(db, trigger="manual")
    assert manual.automation is not None and manual.automation["advanced"] == 1


def test_automation_loop_advances_runs(
    session_factory: sessionmaker[Session], db: Session, clock: Clock
) -> None:
    make_env(db, DeploymentEnvironment.DEV, "partner_api_sync")
    enable(db, "retry-transient")
    detect(db, clock)

    app = create_app(session_factory=session_factory, bootstrap=False, detection=False)
    loop = app.state.automation_loop
    assert loop.name == "automation" and loop.interval_seconds == 30
    summary = loop.run_once(trigger="schedule")
    assert summary.advanced == 1 and loop.last_summary is summary
    db.expire_all()
    assert db.scalars(select(WorkflowRun)).one().status == RunStatus.WAITING  # verifying


def test_status_endpoint(client: TestClient, viewer_headers: dict) -> None:
    body = client.get("/api/v1/automation/status", headers=viewer_headers).json()
    assert body["enabled"] is True and body["force_dry_run"] is False
    assert body["loop_running"] is False  # tests disable the loops
    assert body["interval_seconds"] == 30 and body["last_tick"] is None
