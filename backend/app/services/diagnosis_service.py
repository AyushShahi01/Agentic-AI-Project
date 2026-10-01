"""Diagnose once, persist it (aiplan1 §1): the stored `Incident.diagnosis` is what page views and
workflow runs read, so neither calls the model on the request path.

* `diagnose_and_store` runs when new TASK_LOG evidence arrives (detection) or when a workflow
  needs a diagnosis that is missing. It never overwrites an operator correction.
* `stored_or_backfill` is the read path: old incidents without a stored diagnosis get a
  regex-only one on first read.
* `set_override` records an operator correction (event + audit); it is also labeled data
  (see scripts/export_task_logs.py).
"""

import uuid
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.connectors.ml_client import get_ml_client
from app.core.config import get_settings
from app.core.exceptions import BadRequestError, ConflictError
from app.db.base import utcnow
from app.detection.types import EvidenceKind, IncidentType
from app.diagnosis import hybrid
from app.diagnosis.hybrid import ModelClassifier
from app.diagnosis.log_classifier import (
    CATEGORY_LABELS,
    RETRYABLE,
    FailureCategory,
    classify_logs,
)
from app.models.incident import Incident, IncidentEvidence
from app.models.user import User
from app.services import audit_service, incident_service


def model_client() -> ModelClassifier | None:
    return get_ml_client() if get_settings().ML_SERVICE_ENABLED else None


def task_logs(db: Session, incident: Incident) -> list[str]:
    """The incident's task logs, newest first (flushes so just-attached evidence is included)."""
    db.flush()
    rows = db.scalars(
        select(IncidentEvidence.content)
        .where(
            IncidentEvidence.incident_id == incident.id,
            IncidentEvidence.kind == EvidenceKind.TASK_LOG,
            IncidentEvidence.content.is_not(None),
        )
        .order_by(IncidentEvidence.collected_at.desc())
    ).all()
    return [r for r in rows if r]


def is_operator(diagnosis: dict[str, Any] | None) -> bool:
    return bool(diagnosis) and diagnosis.get("source") == "operator"  # type: ignore[union-attr]


def _event_details(payload: dict[str, Any], **extra: Any) -> dict[str, Any]:
    return {
        "category": payload["category"],
        "source": payload["source"],
        "confidence": payload["confidence"],
        "model_version": payload.get("model_version"),
        "ml_status": payload.get("ml_status"),
        **extra,
    }


def diagnose_and_store(
    db: Session,
    incident: Incident,
    *,
    record_event: bool = True,
    client: ModelClassifier | None = None,
) -> dict[str, Any] | None:
    """(Re)diagnose from the stored task logs; operator corrections are kept as they are."""
    if incident.type != IncidentType.DAG_RUN_FAILED:
        return incident.diagnosis
    if is_operator(incident.diagnosis):
        return incident.diagnosis
    payload, _ = hybrid.decide(
        task_logs(db, incident),
        client if client is not None else model_client(),
        get_settings().ML_MIN_CONFIDENCE,
    )
    incident.diagnosis = payload
    if record_event:
        incident_service.add_event(db, incident, "diagnosed", details=_event_details(payload))
    return payload


def ensure(db: Session, incident: Incident, *, record_event: bool = True) -> dict[str, Any] | None:
    """The stored diagnosis, computing (with the model, if enabled) only when missing."""
    if incident.diagnosis is None:
        return diagnose_and_store(db, incident, record_event=record_event)
    return incident.diagnosis


def stored_or_backfill(db: Session, incident: Incident) -> dict[str, Any] | None:
    """Read path for the incident page. Never calls the model: a missing diagnosis (incident from
    before diagnoses were stored) is backfilled regex-only and persisted."""
    if incident.diagnosis is not None or incident.type != IncidentType.DAG_RUN_FAILED:
        return incident.diagnosis
    payload, _ = hybrid.decide(task_logs(db, incident), None, 1.0)
    payload["ml_status"] = "skipped"
    incident.diagnosis = payload
    incident_service.add_event(
        db, incident, "diagnosed", details=_event_details(payload, backfill=True)
    )
    db.commit()
    return payload


def set_override(
    db: Session,
    incident_id: uuid.UUID,
    category: str,
    note: str | None,
    *,
    actor: User,
    ip_address: str | None,
) -> Incident:
    try:
        chosen = FailureCategory(category)
    except ValueError as exc:
        raise BadRequestError(f"Unknown category {category!r}", code="invalid_category") from exc
    incident = incident_service.get_incident(db, incident_id)
    if incident.type != IncidentType.DAG_RUN_FAILED:
        raise ConflictError(
            "Only failed-run incidents have a diagnosis", code="no_diagnosis"
        )
    previous = incident.diagnosis or stored_or_backfill(db, incident) or {}
    if is_operator(previous):
        previous = previous.get("previous") or {}
    regex = classify_logs(task_logs(db, incident))
    payload = {
        "category": chosen.value,
        "label": CATEGORY_LABELS[chosen],
        "retryable": chosen in RETRYABLE,
        "confidence": 1.0,
        "rule": "operator",
        "matched_line": previous.get("matched_line") or regex.matched_line,
        "source": "operator",
        "model_version": previous.get("model_version"),
        "top_predictions": previous.get("top_predictions") or [],
        "suggestion": None,
        "ml_status": previous.get("ml_status"),
        "diagnosed_at": utcnow().isoformat(),
        "note": note,
        "corrected_by": actor.full_name,
        # What the system said before the first correction (kept across re-corrections).
        "previous": {
            k: previous.get(k)
            for k in ("category", "source", "confidence", "model_version", "matched_line",
                      "top_predictions", "ml_status", "suggestion")
        } if previous else None,
    }
    incident.diagnosis = payload
    details = {
        "category": chosen.value,
        "previous_category": previous.get("category"),
        "previous_source": previous.get("source"),
        "note": note,
    }
    incident_service.add_event(db, incident, "diagnosis_corrected", actor=actor, details=details)
    audit_service.record(
        db,
        action="incident.diagnosis_corrected",
        entity_type="incident",
        entity_id=incident.id,
        actor=actor,
        details={"dag_id": incident.dag_id, **details},
        ip_address=ip_address,
    )
    db.commit()
    return incident
