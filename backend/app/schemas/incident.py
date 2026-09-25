import uuid
from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from app.detection.types import (
    EvidenceKind,
    IncidentResolution,
    IncidentSeverity,
    IncidentStatus,
    IncidentType,
)


class IncidentRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    connection_id: uuid.UUID
    monitored_dag_id: uuid.UUID
    dag_id: str
    run_id: str | None
    last_run_id: str | None
    type: IncidentType
    severity: IncidentSeverity
    status: IncidentStatus
    title: str
    summary: str | None
    occurrence_count: int
    occurred_at: datetime
    first_seen_at: datetime
    last_seen_at: datetime
    acknowledged_at: datetime | None
    resolved_at: datetime | None
    resolution: IncidentResolution | None
    resolution_note: str | None


class EvidenceRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    kind: EvidenceKind
    source: str
    content: str | None
    data: dict[str, Any]
    truncated: bool
    collected_at: datetime


class IncidentEventRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    event: str
    actor_user_id: uuid.UUID | None
    actor_name: str
    details: dict[str, Any]
    created_at: datetime


class DiagnosisRead(BaseModel):
    category: str
    label: str
    retryable: bool
    confidence: float
    rule: str | None
    matched_line: str | None


class IncidentDetail(IncidentRead):
    evidence: list[EvidenceRead]
    events: list[IncidentEventRead]
    diagnosis: DiagnosisRead | None = None


class OpenCount(BaseModel):
    connection_id: uuid.UUID
    dag_id: str
    type: IncidentType
    count: int


class IncidentSummary(BaseModel):
    open_total: int
    by_severity: dict[str, int]


class ResolveRequest(BaseModel):
    note: str = Field(min_length=1, max_length=2000)


class CycleSummaryRead(BaseModel):
    trigger: str
    started_at: datetime
    finished_at: datetime | None
    duration_ms: int
    connections: int
    dags: int
    opened: int
    recurred: int
    auto_resolved: int
    automation_queued: int = 0
    errors: list[dict[str, str]]
    automation: dict[str, Any] | None = None


class LeaseRead(BaseModel):
    holder: str
    expires_at: datetime


class DetectionStatusRead(BaseModel):
    enabled: bool
    loop_running: bool
    interval_seconds: int
    this_worker: str
    lease: LeaseRead | None
    last_cycle: CycleSummaryRead | None
    last_error: str | None
