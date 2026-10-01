"""API tests with a stub predictor: no GPU or weights needed."""

import pytest
from fastapi.testclient import TestClient

from app import config
from app.model import find_weights

TOKEN = "test-token"


class StubPredictor:
    def __init__(self, ready: bool = True):
        self.ready = ready
        self.model_version = "stub-1" if ready else None
        self.device = "cpu"
        self.calls: list[tuple[list[str], int]] = []

    def predict(self, logs: list[str], top_k: int = 3) -> dict:
        self.calls.append((logs, top_k))
        return {
            "category": "TRANSIENT_NETWORK",
            "confidence": 0.93,
            "top_predictions": [{"category": "TRANSIENT_NETWORK", "score": 0.93},
                                {"category": "TIMEOUT", "score": 0.04}][:top_k],
            "model_version": self.model_version,
            "signal_line": "ConnectionResetError: reset",
            "window": "ConnectionResetError: reset",
            "device": self.device,
            "inference_ms": 1.0,
        }

    def vram_used_mb(self) -> float | None:
        return None


@pytest.fixture(autouse=True)
def _settings(monkeypatch):
    monkeypatch.setenv("ML_SERVICE_TOKEN", TOKEN)
    config.get_settings.cache_clear()
    yield
    config.get_settings.cache_clear()


def _client(predictor: StubPredictor) -> TestClient:
    from app.main import create_app

    return TestClient(create_app(predictor))


def test_health_reports_readiness() -> None:
    with _client(StubPredictor()) as client:
        body = client.get("/v1/health").json()
    assert body == {"ready": True, "model_version": "stub-1", "device": "cpu",
                    "vram_used_mb": None}


def test_classify_requires_token() -> None:
    stub = StubPredictor()
    with _client(stub) as client:
        assert client.post("/v1/classify", json={"logs": ["x"]}).status_code == 401
        bad = client.post("/v1/classify", json={"logs": ["x"]}, headers={"X-ML-Token": "nope"})
        assert bad.status_code == 401
    assert stub.calls == []


def test_unset_server_token_rejects_everything(monkeypatch) -> None:
    monkeypatch.setenv("ML_SERVICE_TOKEN", "")
    config.get_settings.cache_clear()
    with _client(StubPredictor()) as client:
        resp = client.post("/v1/classify", json={"logs": ["x"]}, headers={"X-ML-Token": ""})
    assert resp.status_code == 401


def test_classify_ok_and_top_k_limits() -> None:
    stub = StubPredictor()
    with _client(stub) as client:
        resp = client.post(
            "/v1/classify", json={"logs": ["a", "b"], "top_k": 2}, headers={"X-ML-Token": TOKEN}
        )
        assert resp.status_code == 200
        assert resp.json()["category"] == "TRANSIENT_NETWORK"
        assert len(resp.json()["top_predictions"]) == 2
        too_many = client.post(
            "/v1/classify", json={"logs": ["a"], "top_k": 10}, headers={"X-ML-Token": TOKEN}
        )
        assert too_many.status_code == 422
    assert stub.calls == [(["a", "b"], 2)]


def test_body_size_cap() -> None:
    stub = StubPredictor()
    with _client(stub) as client:
        resp = client.post(
            "/v1/classify", json={"logs": ["x" * 300 * 1024]}, headers={"X-ML-Token": TOKEN}
        )
    assert resp.status_code == 413
    assert stub.calls == []


def test_not_ready_returns_503() -> None:
    with _client(StubPredictor(ready=False)) as client:
        assert client.get("/v1/health").json()["ready"] is False
        resp = client.post("/v1/classify", json={"logs": ["x"]}, headers={"X-ML-Token": TOKEN})
    assert resp.status_code == 503


def test_find_weights_picks_newest_trained_dir(tmp_path) -> None:
    assert find_weights(tmp_path / "missing") is None
    for name in ("20260101-0000", "20260301-0000", "20260401-0000"):
        (tmp_path / name).mkdir()
    for name in ("20260101-0000", "20260301-0000"):
        (tmp_path / name / "config.json").write_text("{}")
    assert find_weights(tmp_path).name == "20260301-0000"  # 0401 has no model in it
    assert find_weights(tmp_path, "20260101-0000").name == "20260101-0000"
    assert find_weights(tmp_path, "20260401-0000") is None
