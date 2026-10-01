"""Hybrid diagnosis: regex first, the ML model only where regex is weak (aiplan1 §3).

Pure: the model client is injected, so the rule is unit-tested with a fake.

1. regex category not UNKNOWN/CODE_BUG  -> regex result, no model call (ml_status "skipped").
2. otherwise ask the model (if enabled); on any failure -> regex result ("unavailable").
3. model confidence >= threshold and category != UNKNOWN -> model category (source "model").
4. otherwise regex category, model answer attached as a display-only `suggestion`.
(5. operator corrections win; handled by diagnosis_service, which never re-diagnoses them.)

`retryable` is always derived here from the backend's RETRYABLE set, never from the model.
"""

from datetime import UTC, datetime
from typing import Any, Literal, Protocol

from app.diagnosis.log_classifier import (
    CATEGORY_LABELS,
    RETRYABLE,
    Diagnosis,
    FailureCategory,
    classify_logs,
)

WEAK_CATEGORIES = frozenset({FailureCategory.UNKNOWN, FailureCategory.CODE_BUG})
MAX_MODEL_LOGS = 3

MLStatus = Literal["used", "skipped", "unavailable", "disabled"]
Source = Literal["regex", "model", "operator"]


class MLUnavailable(Exception):
    """The model could not be consulted (down, slow, not ready, breaker open)."""


class ModelPrediction(Protocol):
    category: str
    confidence: float
    top_predictions: list[dict[str, Any]]
    model_version: str | None
    signal_line: str | None


class ModelClassifier(Protocol):
    enabled: bool

    def classify(self, logs: list[str], top_k: int = 3) -> ModelPrediction: ...


def _payload(
    diagnosis: Diagnosis,
    *,
    source: Source,
    ml_status: MLStatus,
    model: ModelPrediction | None = None,
    suggestion: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        **diagnosis.as_dict(),
        "category": diagnosis.category.value,
        "source": source,
        "model_version": model.model_version if model else None,
        "top_predictions": list(model.top_predictions) if model else [],
        "suggestion": suggestion,
        "ml_status": ml_status,
        "diagnosed_at": datetime.now(UTC).isoformat(),
    }


def _category(value: str) -> FailureCategory | None:
    try:
        return FailureCategory(value)
    except ValueError:
        return None


def decide(
    logs: list[str | None],
    client: ModelClassifier | None,
    threshold: float,
    *,
    regex: Diagnosis | None = None,
) -> tuple[dict[str, Any], ModelPrediction | None]:
    """Return (stored diagnosis payload, raw model prediction or None)."""
    r = regex or classify_logs(logs)
    if r.category not in WEAK_CATEGORIES:
        return _payload(r, source="regex", ml_status="skipped"), None
    texts = [t for t in logs if t and t.strip()][:MAX_MODEL_LOGS]
    if client is None or not client.enabled:
        return _payload(r, source="regex", ml_status="disabled"), None
    if not texts:
        return _payload(r, source="regex", ml_status="skipped"), None
    try:
        m = client.classify(texts)
    except MLUnavailable:
        return _payload(r, source="regex", ml_status="unavailable"), None

    m_cat = _category(m.category)
    if m_cat is not None and m_cat != FailureCategory.UNKNOWN and m.confidence >= threshold:
        chosen = Diagnosis(
            category=m_cat,
            label=CATEGORY_LABELS[m_cat],
            retryable=m_cat in RETRYABLE,
            confidence=round(m.confidence, 4),
            rule="model",
            matched_line=m.signal_line or r.matched_line,
        )
        return _payload(chosen, source="model", ml_status="used", model=m), m

    suggestion = None
    if m_cat is not None and m_cat not in (r.category, FailureCategory.UNKNOWN):
        suggestion = {
            "category": m_cat.value,
            "label": CATEGORY_LABELS[m_cat],
            "confidence": round(m.confidence, 4),
        }
    if r.matched_line is None and m.signal_line:
        r = Diagnosis(r.category, r.label, r.retryable, r.confidence, r.rule, m.signal_line)
    return _payload(r, source="regex", ml_status="used", model=m, suggestion=suggestion), m
