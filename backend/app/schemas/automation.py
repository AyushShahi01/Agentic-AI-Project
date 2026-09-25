import uuid
from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from app.automation.types import (
    ApprovalStatus,
    NotificationLevel,
    RunStatus,
    StepStatus,
    WorkflowMode,
)
from app.detection.types import IncidentSeverity, IncidentStatus


class NodeTypeRead(BaseModel):
    type: str
    label: str
    category: str
    description: str
    ports: list[str]
    port_labels: dict[str, str]
    config_schema: dict[str, Any]


class TemplateRead(BaseModel):
    key: str
    name: str
    description: str
    graph: dict[str, Any]


class WorkflowRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    key: str | None
    name: str
    description: str | None
    enabled: bool
    mode: WorkflowMode
    version: int
    graph: dict[str, Any]
    created_at: datetime
    updated_at: datetime
    run_count: int = 0
    last_run_at: datetime | None = None


class WorkflowCreate(BaseModel):
    template_key: str | None = Field(default=None, max_length=100)
    name: str | None = Field(default=None, min_length=1, max_length=200)
    description: str | None = Field(default=None, max_length=2000)
    graph: dict[str, Any] | None = None


class WorkflowUpdate(BaseModel):
    name: str | None = Field(default=None, min_length=1, max_length=200)
    description: str | None = Field(default=None, max_length=2000)
    enabled: bool | None = None
    mode: WorkflowMode | None = None
    graph: dict[str, Any] | None = None


class GraphCheck(BaseModel):
    graph: dict[str, Any]


class GraphCheckResult(BaseModel):
    valid: bool
    problems: list[dict[str, str]]
    graph: dict[str, Any] | None


class RunIncidentRef(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    title: str
    dag_id: str
    severity: IncidentSeverity
    status: IncidentStatus


class WorkflowRunRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    workflow_id: uuid.UUID
    workflow_name: str
    workflow_version: int
    incident_id: uuid.UUID | None
    incident: RunIncidentRef | None
    trigger_event: str
    status: RunStatus
    dry_run: bool
    current_node: str | None
    wake_at: datetime | None
    error: str | None
    started_at: datetime | None
    finished_at: datetime | None
    created_at: datetime
    context: dict[str, Any]


class WorkflowStepRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    node_id: str
    node_type: str
    status: StepStatus
    port: str | None
    output: dict[str, Any]
    message: str | None
    started_at: datetime
    finished_at: datetime | None


class ApprovalRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    run_id: uuid.UUID
    incident_id: uuid.UUID | None
    incident: RunIncidentRef | None
    workflow_name: str
    node_id: str
    title: str
    summary: str | None
    proposed_action: dict[str, Any]
    status: ApprovalStatus
    expires_at: datetime
    decided_by: uuid.UUID | None
    decided_by_name: str | None
    decided_at: datetime | None
    comment: str | None
    created_at: datetime


class WorkflowRunDetail(WorkflowRunRead):
    graph: dict[str, Any]
    steps: list[WorkflowStepRead]
    approvals: list[ApprovalRead]


class DecisionRequest(BaseModel):
    comment: str | None = Field(default=None, max_length=2000)


class NotificationRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    run_id: uuid.UUID | None
    incident_id: uuid.UUID | None
    level: NotificationLevel
    title: str
    body: str | None
    read_at: datetime | None
    created_at: datetime


class AutomationSummary(BaseModel):
    pending_approvals: int
    active_runs: int
    unread_notifications: int
    enabled_workflows: int


class TickSummaryRead(BaseModel):
    enqueued: int
    advanced: int
    completed: int
    waiting: int
    failed: int
    duration_ms: int
    errors: list[dict[str, str]]


class MarkedRead(BaseModel):
    updated: int
