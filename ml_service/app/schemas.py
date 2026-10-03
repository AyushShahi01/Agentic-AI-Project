from pydantic import BaseModel, Field

# Must match backend/app/diagnosis/log_classifier.py FailureCategory. Order = model label ids.
CATEGORIES: tuple[str, ...] = (
    "AUTH",
    "DATA_INTEGRITY",
    "SCHEMA",
    "CODE_BUG",
    "RESOURCE",
    "TIMEOUT",
    "TRANSIENT_NETWORK",
    "UPSTREAM_MISSING",
    "UNKNOWN",
)


class ClassifyRequest(BaseModel):
    logs: list[str] = Field(min_length=1, max_length=10)
    top_k: int = Field(default=3, ge=1, le=len(CATEGORIES))


class ScoredCategory(BaseModel):
    category: str
    score: float


class ClassifyResponse(BaseModel):
    category: str
    confidence: float
    top_predictions: list[ScoredCategory]
    model_version: str
    signal_line: str | None
    window: str  # the preprocessed text the model saw (for the Log Analyzer)
    device: str
    inference_ms: float


class HealthResponse(BaseModel):
    ready: bool
    model_version: str | None
    device: str
    vram_used_mb: float | None
