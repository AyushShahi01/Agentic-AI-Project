"""Automation vocabulary shared by the graph, engine, models and API (no framework imports)."""

from enum import StrEnum


class WorkflowMode(StrEnum):
    LIVE = "LIVE"
    DRY_RUN = "DRY_RUN"


class RunStatus(StrEnum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    WAITING = "WAITING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"

    @property
    def is_terminal(self) -> bool:
        return self in (RunStatus.COMPLETED, RunStatus.FAILED, RunStatus.CANCELLED)


ACTIVE_RUN_STATUSES = (RunStatus.PENDING, RunStatus.RUNNING, RunStatus.WAITING)


class StepStatus(StrEnum):
    RUNNING = "RUNNING"
    WAITING = "WAITING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"


class ApprovalStatus(StrEnum):
    PENDING = "PENDING"
    APPROVED = "APPROVED"
    REJECTED = "REJECTED"
    EXPIRED = "EXPIRED"


class NotificationLevel(StrEnum):
    INFO = "INFO"
    WARNING = "WARNING"
    CRITICAL = "CRITICAL"


class TriggerEvent(StrEnum):
    OPENED = "opened"
    RECURRED = "recurred"
    STALE = "stale"
