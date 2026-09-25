"""Evidence records built from Airflow DTOs, with secret scrubbing for log text."""

import re
from dataclasses import dataclass, field
from typing import Any

from app.detection.types import EvidenceKind
from app.orchestration.airflow.base import AirflowDagRun, AirflowTaskInstance, TaskLog

REDACTED = "***"

_SECRET_PATTERNS: list[tuple[re.Pattern[str], str]] = [
    # scheme://user:password@host  ->  scheme://user:***@host
    (re.compile(r"(\b[a-z][a-z0-9+.-]*://[^:/\s@]+:)([^@\s]+)(@)", re.I), rf"\1{REDACTED}\3"),
    # Authorization: Bearer xxx / Basic xxx
    (re.compile(r"(authorization\s*[:=]\s*)(\S+(?:\s+\S+)?)", re.I), rf"\1{REDACTED}"),
    # password=..., secret: ..., token="...", api_key=...
    (
        re.compile(
            r"""((?:password|passwd|pwd|secret|token|api[_-]?key|access[_-]?key)\w*"""
            r"""["']?\s*[:=]\s*["']?)([^\s"',;]+)""",
            re.I,
        ),
        rf"\1{REDACTED}",
    ),
]


def scrub(text: str) -> str:
    for pattern, replacement in _SECRET_PATTERNS:
        text = pattern.sub(replacement, text)
    return text


@dataclass(frozen=True)
class EvidenceRecord:
    kind: EvidenceKind
    source: str
    content: str | None = None
    data: dict[str, Any] = field(default_factory=dict)
    truncated: bool = False


def run_source(run_id: str) -> str:
    return f"run:{run_id}"


def run_metadata(run: AirflowDagRun, **extra: Any) -> EvidenceRecord:
    return EvidenceRecord(
        kind=EvidenceKind.RUN_METADATA,
        source=run_source(run.run_id),
        data={**run.as_dict(), **extra},
    )


def task_instances(run_id: str, instances: list[AirflowTaskInstance]) -> EvidenceRecord | None:
    problems = [ti for ti in instances if ti.state not in ("success", "skipped")]
    if not problems:
        return None
    return EvidenceRecord(
        kind=EvidenceKind.TASK_INSTANCE,
        source=run_source(run_id),
        data={"task_instances": [ti.as_dict() for ti in problems]},
    )


def task_log(run_id: str, instance: AirflowTaskInstance, log: TaskLog) -> EvidenceRecord:
    source = f"{run_source(run_id)} task:{instance.task_id} try:{instance.try_number}"
    if not log.available:
        return EvidenceRecord(
            kind=EvidenceKind.TASK_LOG,
            source=source,
            content=None,
            data={"available": False, "reason": "Log not available from Airflow"},
        )
    return EvidenceRecord(
        kind=EvidenceKind.TASK_LOG,
        source=source,
        content=scrub(log.content),
        data={"task_id": instance.task_id, "try_number": instance.try_number},
        truncated=log.truncated,
    )


def unavailable(run_id: str, kind: EvidenceKind, reason: str) -> EvidenceRecord:
    return EvidenceRecord(
        kind=kind,
        source=f"{run_source(run_id)} unavailable",
        data={"available": False, "reason": reason},
    )


def failure_summary(run: AirflowDagRun, instances: list[AirflowTaskInstance]) -> str:
    failed = [ti for ti in instances if ti.state == "failed"]
    when = run.sort_date.isoformat() if run.sort_date else "unknown time"
    if not failed:
        return f"Run {run.run_id} ({when}) finished in state failed."
    tasks = ", ".join(f"{ti.task_id} (try {ti.try_number})" for ti in failed)
    return f"Run {run.run_id} ({when}) failed. Failed task(s): {tasks}."
