"""Executors for workflow node types (see app/automation/graph.py for the catalog).

Each executor takes a NodeContext and the node, performs its work (policy-checked for actions),
and returns a NodeResult naming the output port to follow, or a time to wait until.
"""

import logging
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any
from urllib.parse import urlsplit

import httpx
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.automation import policy
from app.automation.graph import ACTION_TYPES, Graph, Node
from app.automation.render import render
from app.automation.types import ApprovalStatus, NotificationLevel
from app.core.config import Settings
from app.detection.types import (
    IncidentResolution,
    IncidentSeverity,
    IncidentStatus,
)
from app.models.airflow import AirflowConnection, MonitoredDag
from app.models.automation import Approval, Notification, WorkflowRun, WorkflowStep
from app.models.incident import Incident
from app.orchestration.airflow.base import (
    AirflowAdapter,
    AirflowAdapterError,
    ConnectionStatus,
    ConnectionTestResult,
)
from app.orchestration.airflow.factory import run_async
from app.services import airflow_service, audit_service, incident_service

logger = logging.getLogger(__name__)

# Test hook: inject an httpx transport for webhook delivery.
webhook_transport: httpx.BaseTransport | None = None

ACTIVE_RUN_STATES = frozenset({"queued", "running", "restarting", "up_for_retry", "scheduled"})


@dataclass
class NodeResult:
    port: str | None = None
    output: dict[str, Any] = field(default_factory=dict)
    message: str | None = None
    wait_until: datetime | None = None


class NodeError(Exception):
    """The node could not run at all (fails the workflow run)."""


@dataclass
class NodeContext:
    db: Session
    run: WorkflowRun
    graph: Graph
    incident: Incident
    now: datetime
    settings: Settings
    _adapter: AirflowAdapter | None = None

    @property
    def connection(self) -> AirflowConnection:
        conn = self.db.get(AirflowConnection, self.incident.connection_id)
        if conn is None:
            raise NodeError("The incident's Airflow connection no longer exists")
        return conn

    @property
    def dag(self) -> MonitoredDag | None:
        return self.db.get(MonitoredDag, self.incident.monitored_dag_id)

    @property
    def environment(self) -> str:
        return str(self.connection.environment)

    @property
    def dry_run(self) -> bool:
        return self.run.dry_run or self.settings.AUTOMATION_FORCE_DRY_RUN

    def adapter(self) -> AirflowAdapter:
        if self._adapter is None:
            self._adapter = airflow_service.adapter_for(self.connection)
        return self._adapter

    def set_context(self, key: str, value: Any) -> None:
        # Reassign so SQLAlchemy sees the JSON change.
        self.run.context = {**(self.run.context or {}), key: value}

    def get_context(self, key: str, default: Any = None) -> Any:
        return (self.run.context or {}).get(key, default)

    def node_state(self, node_id: str) -> dict[str, Any]:
        return dict(self.get_context("nodes", {}).get(node_id, {}))

    def set_node_state(self, node_id: str, state: dict[str, Any]) -> None:
        self.set_context("nodes", {**self.get_context("nodes", {}), node_id: state})

    def event(self, event: str, /, **details: Any) -> None:
        incident_service.add_event(
            self.db,
            self.incident,
            event,
            details={"workflow": self.run.workflow_name, "run_id": str(self.run.id), **details},
        )

    def audit(self, action: str, /, **details: Any) -> None:
        audit_service.record(
            self.db,
            action=action,
            entity_type="workflow_run",
            entity_id=self.run.id,
            details={
                "workflow": self.run.workflow_name,
                "incident_id": str(self.incident.id),
                "dag_id": self.incident.dag_id,
                "dry_run": self.dry_run,
                **details,
            },
        )

    def render_values(self) -> dict[str, Any]:
        incident = self.incident
        return {
            "incident": {
                "id": str(incident.id),
                "title": incident.title,
                "dag_id": incident.dag_id,
                "type": str(incident.type),
                "severity": str(incident.severity),
                "status": str(incident.status),
                "occurrence_count": incident.occurrence_count,
                "run_id": incident.last_run_id or incident.run_id or "",
            },
            "diagnosis": self.get_context("diagnosis", {}),
            "workflow": {"name": self.run.workflow_name},
            "environment": self.environment,
            "last_error": self.get_context("last_error", ""),
        }


# ---------------------------------------------------------------------- triggers & logic


def _trigger(ctx: NodeContext, node: Node) -> NodeResult:
    return NodeResult("next", message=f"Triggered by incident {ctx.run.trigger_event}")


def _filter(ctx: NodeContext, node: Node) -> NodeResult:
    cfg = node.config
    incident = ctx.incident
    checks: list[str] = []
    failed: list[str] = []

    def check(ok: bool, label: str) -> None:
        (checks if ok else failed).append(label)

    if cfg["environments"]:
        check(ctx.environment in cfg["environments"], f"environment {ctx.environment}")
    if cfg["incident_types"]:
        check(str(incident.type) in cfg["incident_types"], f"type {incident.type}")
    if cfg["min_severity"]:
        check(
            incident.severity.rank >= IncidentSeverity(cfg["min_severity"]).rank,
            f"severity {incident.severity} >= {cfg['min_severity']}",
        )
    if cfg["dag_ids"]:
        check(incident.dag_id in cfg["dag_ids"], f"DAG {incident.dag_id}")
    if cfg["tags_any"]:
        tags = set(ctx.dag.tags if ctx.dag else [])
        check(bool(tags & set(cfg["tags_any"])), f"tags {sorted(tags)}")
    if cfg["min_occurrences"] is not None:
        check(
            incident.occurrence_count >= cfg["min_occurrences"],
            f"occurrences {incident.occurrence_count} >= {cfg['min_occurrences']}",
        )
    if cfg["max_occurrences"] is not None:
        check(
            incident.occurrence_count <= cfg["max_occurrences"],
            f"occurrences {incident.occurrence_count} <= {cfg['max_occurrences']}",
        )
    if cfg["diagnosis_categories"]:
        category = ctx.get_context("diagnosis", {}).get("category")
        check(category in cfg["diagnosis_categories"], f"diagnosis {category or 'none'}")

    matched = not failed
    message = ("Matched: " if matched else "Did not match: ") + ", ".join(
        failed if not matched else checks or ["no criteria"]
    )
    return NodeResult("true" if matched else "false", {"passed": checks, "failed": failed}, message)


def _classify(ctx: NodeContext, node: Node) -> NodeResult:
    diagnosis = incident_service.diagnose(ctx.incident).as_dict()
    ctx.set_context("diagnosis", diagnosis)
    ctx.event(
        "diagnosed",
        category=diagnosis["category"],
        label=diagnosis["label"],
        retryable=diagnosis["retryable"],
        matched_line=diagnosis["matched_line"],
    )
    return NodeResult("next", diagnosis, f"Diagnosis: {diagnosis['label']}")


def _dag_state(ctx: NodeContext, node: Node) -> NodeResult:
    dag_id = ctx.incident.dag_id
    try:
        dag = run_async(lambda: ctx.adapter().get_dag(dag_id))
        if dag is None:
            raise NodeError(f"DAG {dag_id} no longer exists in Airflow")
        if dag.is_paused:
            return NodeResult("paused", {"is_paused": True}, f"{dag_id} is paused")
        runs = run_async(
            lambda: ctx.adapter().list_dag_runs(dag_id, since=ctx.now - timedelta(hours=24))
        )
    except AirflowAdapterError as exc:
        raise NodeError(f"Could not read DAG state: {exc.result.message}") from exc
    active = [r.run_id for r in runs if r.state in ACTIVE_RUN_STATES]
    if active:
        return NodeResult("busy", {"active_runs": active}, f"A run is already active: {active[0]}")
    return NodeResult("ready", {"is_paused": False}, f"{dag_id} is idle and not paused")


# ---------------------------------------------------------------------- approval


def _proposed_action(ctx: NodeContext, node: Node) -> dict[str, Any]:
    nxt = ctx.graph.next_node(node.id, "approved")
    target = ctx.graph.nodes.get(nxt) if nxt else None
    if target is None:
        return {}
    return {"node_id": target.id, "type": target.type, "config": target.config}


_ACTION_TEXT = {
    "action.clear_failed_tasks": "retry the failed tasks of run {run_id}",
    "action.trigger_dag_run": "start a new run of {dag_id}",
    "action.set_dag_paused": "pause {dag_id}",
}


def describe_action(action: dict[str, Any], incident: Incident) -> str:
    template = _ACTION_TEXT.get(action.get("type", ""), "continue the workflow")
    if action.get("type") == "action.set_dag_paused" and not action["config"].get("paused", True):
        template = "unpause {dag_id}"
    return template.format(run_id=incident.last_run_id or incident.run_id, dag_id=incident.dag_id)


def _approval(ctx: NodeContext, node: Node) -> NodeResult:
    state = ctx.node_state(node.id)
    approval_id = state.get("approval_id")

    if approval_id is None:
        required_envs = node.config["required_environments"]
        if ctx.dry_run:
            return NodeResult("approved", {"auto": "dry_run"}, "Dry run: approval skipped")
        if required_envs and ctx.environment not in required_envs:
            return NodeResult(
                "approved",
                {"auto": "not_required", "environment": ctx.environment},
                f"No approval needed in {ctx.environment}",
            )
        action = _proposed_action(ctx, node)
        what = describe_action(action, ctx.incident)
        diagnosis = ctx.get_context("diagnosis", {})
        summary = f"The workflow wants to {what} on {ctx.environment}."
        if diagnosis:
            summary += f" Diagnosis: {diagnosis.get('label')}."
            if diagnosis.get("matched_line"):
                summary += f" Evidence: {diagnosis['matched_line']}"
        expires = ctx.now + timedelta(minutes=node.config["timeout_minutes"])
        approval = Approval(
            id=uuid.uuid4(),
            run_id=ctx.run.id,
            incident_id=ctx.incident.id,
            node_id=node.id,
            title=f"{ctx.incident.dag_id}: {what}"[:300],
            summary=summary,
            proposed_action={**action, "description": what, "environment": ctx.environment},
            status=ApprovalStatus.PENDING,
            expires_at=expires,
        )
        ctx.db.add(approval)
        ctx.db.add(
            Notification(
                run_id=ctx.run.id,
                incident_id=ctx.incident.id,
                level=NotificationLevel.WARNING,
                title=f"Approval needed: {approval.title}"[:300],
                body=summary,
                created_at=ctx.now,
            )
        )
        ctx.set_node_state(node.id, {"approval_id": str(approval.id)})
        ctx.event("approval_requested", approval_id=str(approval.id), action=what)
        ctx.audit("approval.requested", approval_id=str(approval.id), action=what)
        return NodeResult(
            output={"approval_id": str(approval.id), "expires_at": expires.isoformat()},
            message=f"Waiting for approval to {what}",
            wait_until=expires,
        )

    approval = ctx.db.get(Approval, uuid.UUID(approval_id))
    if approval is None:
        raise NodeError("Approval record disappeared")
    if approval.status == ApprovalStatus.PENDING:
        if ctx.now < approval.expires_at:
            return NodeResult(
                output={"approval_id": approval_id},
                message="Still waiting for approval",
                wait_until=approval.expires_at,
            )
        approval.status = ApprovalStatus.EXPIRED
        approval.decided_at = ctx.now
        ctx.event("approval_expired", approval_id=approval_id)
        ctx.audit("approval.expired", approval_id=approval_id)
    output = {
        "approval_id": approval_id,
        "status": str(approval.status),
        "decided_by": approval.decided_by_name,
        "comment": approval.comment,
    }
    if approval.status == ApprovalStatus.APPROVED:
        ctx.set_context("approval_granted", True)
        return NodeResult("approved", output, f"Approved by {approval.decided_by_name}")
    ctx.set_context("last_error", f"Approval {str(approval.status).lower()}.")
    return NodeResult("rejected", output, f"Approval {str(approval.status).lower()}")


# ---------------------------------------------------------------------- actions


def _actions_last_24h(ctx: NodeContext) -> int:
    since = ctx.now - timedelta(hours=24)
    return (
        ctx.db.scalar(
            select(func.count(WorkflowStep.id))
            .join(WorkflowRun, WorkflowRun.id == WorkflowStep.run_id)
            .join(Incident, Incident.id == WorkflowRun.incident_id)
            .where(
                Incident.connection_id == ctx.incident.connection_id,
                Incident.dag_id == ctx.incident.dag_id,
                WorkflowStep.node_type.in_(ACTION_TYPES),
                WorkflowStep.port == "success",
                WorkflowStep.finished_at >= since,
                WorkflowRun.dry_run.is_(False),
            )
        )
        or 0
    )


def _run_action(
    ctx: NodeContext,
    node: Node,
    description: str,
    execute: Callable[[], dict[str, Any]],
    simulated_target: dict[str, Any] | None = None,
) -> NodeResult:
    decision = policy.check_action(
        node.type,
        environment=ctx.environment,
        dry_run=ctx.dry_run,
        approved=bool(ctx.get_context("approval_granted")),
        actions_last_24h=_actions_last_24h(ctx),
        max_per_day=ctx.settings.AUTOMATION_MAX_ACTIONS_PER_DAG_PER_DAY,
    )
    if not decision.allowed:
        reason = "; ".join(decision.reasons)
        ctx.set_context("last_error", f"Not allowed to {description}: {reason}.")
        ctx.event("action_denied", action=description, reasons=decision.reasons)
        ctx.audit("automation.action_denied", action=node.type, reasons=decision.reasons)
        return NodeResult("failed", {"policy": decision.as_dict()}, f"Blocked by policy: {reason}")

    if decision.simulate:
        ctx.set_context("action_target", {**(simulated_target or {}), "simulated": True})
        ctx.event("action_simulated", action=description)
        return NodeResult(
            "success",
            {"policy": decision.as_dict(), "simulated": True},
            f"Dry run: would {description}",
        )

    try:
        output = execute()
    except AirflowAdapterError as exc:
        message = exc.result.message
        ctx.set_context("last_error", f"Could not {description}: {message}")
        ctx.event("action_failed", action=description, error=message)
        ctx.audit("automation.action_failed", action=node.type, error=message)
        return NodeResult("failed", {"error": message}, f"Airflow error: {message}")

    ctx.event("action_executed", action=description, **output)
    ctx.audit("automation.action_executed", action=node.type, **output)
    return NodeResult("success", {"policy": decision.as_dict(), **output}, f"Done: {description}")


def _clear_failed_tasks(ctx: NodeContext, node: Node) -> NodeResult:
    incident = ctx.incident
    run_id = incident.last_run_id or incident.run_id
    if not run_id:
        ctx.set_context("last_error", "There is no failed run to retry.")
        return NodeResult("failed", message="There is no failed run to retry")

    def execute() -> dict[str, Any]:
        cleared = run_async(
            lambda: ctx.adapter().clear_task_instances(
                incident.dag_id,
                run_id,
                only_failed=True,
                include_downstream=node.config["include_downstream"],
            )
        )
        if not cleared:
            raise AirflowAdapterError(_adapter_result(f"No failed tasks to clear in run {run_id}"))
        ctx.set_context(
            "action_target", {"dag_id": incident.dag_id, "run_id": run_id, "action": "clear"}
        )
        return {"run_id": run_id, "cleared_tasks": cleared}

    return _run_action(
        ctx,
        node,
        f"retry the failed tasks of run {run_id}",
        execute,
        {"dag_id": incident.dag_id, "run_id": run_id, "action": "clear"},
    )


def _trigger_dag_run(ctx: NodeContext, node: Node) -> NodeResult:
    dag_id = ctx.incident.dag_id

    def execute() -> dict[str, Any]:
        note = f"Triggered by automation '{ctx.run.workflow_name}' for incident {ctx.incident.id}"
        run = run_async(lambda: ctx.adapter().trigger_dag_run(dag_id, note=note))
        ctx.set_context(
            "action_target", {"dag_id": dag_id, "run_id": run.run_id, "action": "trigger"}
        )
        return {"run_id": run.run_id}

    return _run_action(ctx, node, f"start a new run of {dag_id}", execute, {"dag_id": dag_id})


def _set_dag_paused(ctx: NodeContext, node: Node) -> NodeResult:
    dag_id = ctx.incident.dag_id
    paused = node.config["paused"]

    def execute() -> dict[str, Any]:
        run_async(lambda: ctx.adapter().set_dag_paused(dag_id, paused))
        dag = ctx.dag
        if dag is not None:
            dag.is_paused = paused
        ctx.set_context("action_target", {"dag_id": dag_id, "action": "pause"})
        return {"dag_id": dag_id, "paused": paused}

    verb = "pause" if paused else "unpause"
    return _run_action(ctx, node, f"{verb} {dag_id}", execute, {"dag_id": dag_id})


def _adapter_result(message: str) -> ConnectionTestResult:
    return ConnectionTestResult(status=ConnectionStatus.AIRFLOW_ERROR, message=message)


# ---------------------------------------------------------------------- verify


def _verify(ctx: NodeContext, node: Node) -> NodeResult:
    target = ctx.get_context("action_target") or {}
    if target.get("simulated") or ctx.dry_run:
        return NodeResult("success", {"simulated": True}, "Dry run: verification skipped")
    run_id = target.get("run_id")
    if not run_id:
        ctx.set_context("last_error", "Nothing to verify: the previous action started no run.")
        return NodeResult("failed", message="Nothing to verify")

    state = ctx.node_state(node.id)
    if "deadline" not in state:
        deadline = ctx.now + timedelta(minutes=node.config["timeout_minutes"])
        state = {"deadline": deadline.isoformat(), "checks": 0}
    deadline = datetime.fromisoformat(state["deadline"])
    state["checks"] = int(state.get("checks", 0)) + 1
    ctx.set_node_state(node.id, state)

    dag_id = target["dag_id"]
    try:
        run = run_async(lambda: ctx.adapter().get_dag_run(dag_id, run_id))
    except AirflowAdapterError as exc:
        if ctx.now >= deadline:
            ctx.set_context("last_error", f"Could not check run {run_id}: {exc.result.message}")
            ctx.event("verification_failed", run=run_id, reason=exc.result.message)
            return NodeResult("failed", {"error": exc.result.message}, "Could not verify")
        return _verify_wait(ctx, deadline, f"Airflow unavailable, retrying: {exc.result.message}")

    if run is None:
        ctx.set_context("last_error", f"Run {run_id} no longer exists.")
        ctx.event("verification_failed", run=run_id, reason="run not found")
        return NodeResult("failed", message=f"Run {run_id} not found")
    if run.state == "success":
        ctx.event("verified", run=run_id, state=run.state)
        return NodeResult("success", {"run_id": run_id, "state": run.state}, "Run succeeded")
    if run.state == "failed":
        ctx.set_context("last_error", f"Run {run_id} failed again after the fix.")
        ctx.event("verification_failed", run=run_id, state=run.state)
        return NodeResult("failed", {"run_id": run_id, "state": run.state}, "Run failed again")
    if ctx.now >= deadline:
        ctx.set_context("last_error", f"Run {run_id} did not finish in time (state {run.state}).")
        ctx.event("verification_failed", run=run_id, state=run.state, reason="timeout")
        return NodeResult("failed", {"run_id": run_id, "state": run.state}, "Timed out")
    return _verify_wait(ctx, deadline, f"Run {run_id} is {run.state}; checking again")


def _verify_wait(ctx: NodeContext, deadline: datetime, message: str) -> NodeResult:
    poll = timedelta(seconds=ctx.settings.AUTOMATION_VERIFY_POLL_SECONDS)
    return NodeResult(message=message, wait_until=min(ctx.now + poll, deadline))


# ---------------------------------------------------------------------- incident & notify


def _incident_update(ctx: NodeContext, node: Node) -> NodeResult:
    incident = ctx.incident
    operation = node.config["operation"]
    note = render(node.config["note"] or "", ctx.render_values()).strip() or None

    if operation == "note":
        ctx.event("automation_note", note=note or "")
        return NodeResult("next", {"note": note}, "Note added")

    if ctx.dry_run:
        return NodeResult("next", {"simulated": True}, f"Dry run: would {operation} the incident")

    if operation == "resolve":
        if incident.status == IncidentStatus.RESOLVED:
            return NodeResult("next", {"skipped": True}, "Incident was already resolved")
        incident_service.mark_resolved(incident, IncidentResolution.AUTO_REMEDIATED, note=note)
        ctx.event("auto_remediated", note=note or "")
        ctx.audit("incident.auto_remediated", note=note)
        return NodeResult("next", {"status": "RESOLVED"}, "Incident resolved")

    if operation == "escalate":
        if incident.status == IncidentStatus.RESOLVED:
            return NodeResult("next", {"skipped": True}, "Incident is resolved; not escalated")
        levels = list(IncidentSeverity)
        before = incident.severity
        after = levels[min(before.rank + 1, len(levels) - 1)]
        if after == before:
            return NodeResult("next", {"severity": str(after)}, f"Already {after}")
        incident.severity = after
        ctx.event("escalated", severity={"from": str(before), "to": str(after)})
        ctx.audit("incident.escalated", severity_from=str(before), severity_to=str(after))
        return NodeResult("next", {"from": str(before), "to": str(after)}, f"Escalated to {after}")

    # acknowledge
    if incident.status != IncidentStatus.OPEN:
        return NodeResult("next", {"skipped": True}, f"Incident is {incident.status}")
    incident.status = IncidentStatus.ACKNOWLEDGED
    incident.acknowledged_at = ctx.now
    ctx.event("acknowledged")
    return NodeResult("next", {"status": "ACKNOWLEDGED"}, "Incident acknowledged")


def _host_allowed(url: str, allowed: list[str]) -> bool:
    host = (urlsplit(url).hostname or "").lower()
    return not allowed or host in {h.lower() for h in allowed}


def _notify(ctx: NodeContext, node: Node) -> NodeResult:
    cfg = node.config
    values = ctx.render_values()
    title = render(cfg["title"], values).strip()[:300] or ctx.incident.title
    body = render(cfg["message"], values).strip()
    level = NotificationLevel(cfg["level"])

    if cfg["channel"] == "in_app":
        prefix = "[Dry run] " if ctx.dry_run else ""
        ctx.db.add(
            Notification(
                run_id=ctx.run.id,
                incident_id=ctx.incident.id,
                level=level,
                title=(prefix + title)[:300],
                body=body,
                created_at=ctx.now,
            )
        )
        ctx.event("notified", channel="in_app", title=title)
        return NodeResult("next", {"channel": "in_app", "title": title}, "In-app notification")

    url = cfg["url"] or ""
    if ctx.dry_run:
        return NodeResult("next", {"simulated": True, "url": url}, "Dry run: webhook not sent")
    if not url or not _host_allowed(url, ctx.settings.AUTOMATION_WEBHOOK_ALLOWED_HOSTS):
        ctx.event("notify_failed", channel="webhook", error="host not allowed")
        return NodeResult("next", {"error": "Webhook host is not allowed"}, "Webhook blocked")
    payload = {
        "title": title,
        "message": body,
        "level": str(level),
        "incident": values["incident"],
        "diagnosis": values["diagnosis"],
        "workflow": values["workflow"],
        "run_id": str(ctx.run.id),
        "environment": values["environment"],
    }
    try:
        with httpx.Client(
            timeout=ctx.settings.AUTOMATION_WEBHOOK_TIMEOUT_SECONDS,
            follow_redirects=False,
            transport=webhook_transport,
        ) as client:
            response = client.post(url, json=payload)
        ok = response.is_success
        error = None if ok else f"HTTP {response.status_code}"
    except httpx.HTTPError as exc:
        ok, error = False, type(exc).__name__
    host = urlsplit(url).hostname
    if ok:
        ctx.event("notified", channel="webhook", host=host)
        return NodeResult("next", {"channel": "webhook", "host": host}, "Webhook delivered")
    ctx.event("notify_failed", channel="webhook", host=host, error=error)
    return NodeResult("next", {"channel": "webhook", "error": error}, f"Webhook failed: {error}")


EXECUTORS: dict[str, Callable[[NodeContext, Node], NodeResult]] = {
    "trigger.incident": _trigger,
    "trigger.incident_stale": _trigger,
    "condition.filter": _filter,
    "diagnose.classify_log": _classify,
    "check.dag_state": _dag_state,
    "approval.request": _approval,
    "action.clear_failed_tasks": _clear_failed_tasks,
    "action.trigger_dag_run": _trigger_dag_run,
    "action.set_dag_paused": _set_dag_paused,
    "verify.run_success": _verify,
    "incident.update": _incident_update,
    "notify": _notify,
}
