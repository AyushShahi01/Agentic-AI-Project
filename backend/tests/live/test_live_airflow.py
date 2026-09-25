"""Plan 4: end-to-end against a REAL Airflow (2.10 or 3.x) running the DAGs in
deploy/airflow/dags/agentic_test_dags.py.

    LIVE_AIRFLOW_URL=http://localhost:8080 pytest -m live_airflow

Optional: LIVE_AIRFLOW_USER / LIVE_AIRFLOW_PASSWORD (default admin / admin).
Skipped unless LIVE_AIRFLOW_URL is set. Uses real time, so it takes a minute or two.
"""

import os
import time
from collections.abc import Callable
from datetime import timedelta

import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.automation.types import RunStatus
from app.core.config import get_settings
from app.core.crypto import encrypt_secret
from app.db.base import utcnow
from app.detection.types import IncidentResolution, IncidentStatus
from app.models.airflow import (
    AirflowConnection,
    ConnectionKind,
    DeploymentEnvironment,
    MonitoredDag,
)
from app.models.automation import Notification, Workflow, WorkflowRun
from app.models.incident import Incident
from app.orchestration.airflow.base import AirflowAdapter, AuthType, ConnectionStatus
from app.orchestration.airflow.factory import run_async
from app.services import airflow_service, automation_service, detection_service

URL = os.getenv("LIVE_AIRFLOW_URL")
FLAKY, BAD = "agentic_flaky_once", "agentic_bad_data"
TIMEOUT_SECONDS = 300

pytestmark = [
    pytest.mark.live_airflow,
    pytest.mark.skipif(not URL, reason="LIVE_AIRFLOW_URL not set"),
]


def wait_for(what: str, check: Callable[[], bool], timeout: int = TIMEOUT_SECONDS) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if check():
            return
        time.sleep(3)
    pytest.fail(f"Timed out after {timeout}s waiting for {what}")


@pytest.fixture
def conn(db: Session) -> AirflowConnection:
    conn = AirflowConnection(
        name="live",
        environment=DeploymentEnvironment.DEV,
        kind=ConnectionKind.LIVE,
        base_url=URL,
        auth_type=AuthType.BASIC,
        username=os.getenv("LIVE_AIRFLOW_USER", "admin"),
        encrypted_secret=encrypt_secret(os.getenv("LIVE_AIRFLOW_PASSWORD", "admin")),
    )
    db.add(conn)
    db.commit()
    return conn


def test_real_failure_is_fixed_and_bad_data_is_left_alone(
    db: Session, conn: AirflowConnection, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(get_settings(), "AUTOMATION_VERIFY_POLL_SECONDS", 5)
    adapter: AirflowAdapter = airflow_service.adapter_for(conn)

    # 1. Probe and discover the test DAGs.
    probe = run_async(adapter.probe)
    assert probe.status == ConnectionStatus.HEALTHY, probe.message
    conn.api_version = probe.api_version
    for dag_id in (FLAKY, BAD):
        summary = run_async(lambda d=dag_id: adapter.get_dag(d))
        assert summary is not None, f"{dag_id} not loaded; mount deploy/airflow/dags"
        db.add(
            MonitoredDag(connection_id=conn.id, dag_id=dag_id, is_monitored=True, tags=summary.tags)
        )
    db.commit()
    adapter = airflow_service.adapter_for(conn)  # reuse the detected API version

    # 2. Pause/unpause round trip, then start one run of each DAG.
    run_async(lambda: adapter.set_dag_paused(FLAKY, True))
    assert run_async(lambda: adapter.get_dag(FLAKY)).is_paused is True
    started_at = utcnow() - timedelta(seconds=5)
    run_ids = {}
    for dag_id in (FLAKY, BAD):
        run_async(lambda d=dag_id: adapter.set_dag_paused(d, False))
        run = run_async(lambda d=dag_id: adapter.trigger_dag_run(d, note="agentic live e2e"))
        assert run.run_id
        run_ids[dag_id] = run.run_id

    def state(dag_id: str) -> str | None:
        run = run_async(lambda: adapter.get_dag_run(dag_id, run_ids[dag_id]))
        return run.state if run else None

    wait_for("both runs to fail", lambda: state(FLAKY) == "failed" and state(BAD) == "failed")

    # 3. Turn on the two built-in workflows and detect.
    automation_service.seed_templates(db)
    for key in ("retry-transient", "no-retry-data-code"):
        db.scalar(select(Workflow).where(Workflow.key == key)).enabled = True
    db.commit()
    summary = detection_service.run_cycle(db)
    assert summary.errors == []
    incidents = {i.dag_id: i for i in db.scalars(select(Incident)).all()}
    assert set(incidents) == {FLAKY, BAD}
    for incident in incidents.values():
        assert any(e.kind == "TASK_LOG" and e.content for e in incident.evidence), (
            "the real task log should be collected as evidence"
        )

    # 4. Let automation work until every run is finished.
    def all_runs_done() -> bool:
        automation_service.tick(db)
        db.expire_all()
        runs = db.scalars(select(WorkflowRun)).all()
        return bool(runs) and all(r.status.is_terminal for r in runs)

    wait_for("automation runs to finish", all_runs_done)

    runs = {(r.incident.dag_id, r.workflow.key): r for r in db.scalars(select(WorkflowRun)).all()}
    fixed = runs[(FLAKY, "retry-transient")]
    assert fixed.status == RunStatus.COMPLETED, fixed.error
    assert fixed.context["diagnosis"]["category"] == "TRANSIENT_NETWORK"
    assert fixed.context["outcome"] == "notify_ok.next", [
        (s.node_id, s.port, s.message) for s in fixed.steps
    ]
    assert state(FLAKY) == "success"  # the real rerun succeeded

    flaky, bad = incidents[FLAKY], incidents[BAD]
    db.refresh(flaky)
    db.refresh(bad)
    assert flaky.status == IncidentStatus.RESOLVED
    assert flaky.resolution == IncidentResolution.AUTO_REMEDIATED

    skipped = runs[(BAD, "no-retry-data-code")]
    assert skipped.context["diagnosis"]["category"] == "DATA_INTEGRITY"
    assert runs[(BAD, "retry-transient")].context["outcome"] == "is_retryable.false"
    assert bad.status == IncidentStatus.OPEN and state(BAD) == "failed"  # never retried
    assert any(n.title.startswith("Needs a fix") for n in db.scalars(select(Notification)).all())
    assert all(started_at <= r.created_at for r in runs.values())

    # Leave the DAGs paused so they don't run between test sessions.
    for dag_id in (FLAKY, BAD):
        run_async(lambda d=dag_id: adapter.set_dag_paused(d, True))
