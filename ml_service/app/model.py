"""DistilBERT failure classifier: loads a versioned weights directory and predicts categories.

No zero-shot fallback: without trained weights the predictor reports ``ready = False`` and the
backend keeps using regex alone.
"""

import json
import logging
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from app.preprocess import Window, window
from app.schemas import CATEGORIES

logger = logging.getLogger(__name__)

MAX_LENGTH = 512


class PredictorProtocol(Protocol):
    ready: bool
    model_version: str | None
    device: str

    def predict(self, logs: list[str], top_k: int = 3) -> dict[str, Any]: ...

    def vram_used_mb(self) -> float | None: ...


def ece(probs, labels, bins: int = 10) -> float:
    """Expected calibration error: |accuracy - confidence| averaged over confidence bins."""
    import numpy as np

    probs, labels = np.asarray(probs), np.asarray(labels)
    conf, pred = probs.max(axis=1), probs.argmax(axis=1)
    edges = np.linspace(0.0, 1.0, bins + 1)
    total = 0.0
    for lo, hi in zip(edges[:-1], edges[1:], strict=True):
        mask = (conf > lo) & (conf <= hi)
        if mask.any():
            total += mask.mean() * abs((pred[mask] == labels[mask]).mean() - conf[mask].mean())
    return float(total)


def find_weights(weights_dir: Path, version: str | None = None) -> Path | None:
    """`weights/<version>/` if given, else the newest directory that holds a trained model."""
    if version:
        path = weights_dir / version
        return path if (path / "config.json").exists() else None
    candidates = sorted(
        (p for p in weights_dir.iterdir() if p.is_dir() and (p / "config.json").exists()),
        key=lambda p: p.name,
    ) if weights_dir.exists() else []
    return candidates[-1] if candidates else None


@dataclass
class _Scored:
    window: Window
    probs: list[float]

    @property
    def best(self) -> int:
        return max(range(len(self.probs)), key=self.probs.__getitem__)


class Predictor:
    def __init__(self, weights_dir: Path, version: str | None = None, device: str | None = None):
        import torch

        self._torch = torch
        self.device = device or ("cuda" if torch.cuda.is_available() else "cpu")
        self.ready = False
        self.model_version: str | None = None
        self.labels: list[str] = list(CATEGORIES)
        path = find_weights(weights_dir, version)
        if path is None:
            logger.warning("No trained weights under %s; classifier is not ready", weights_dir)
            return

        from transformers import AutoModelForSequenceClassification, AutoTokenizer

        self.tokenizer = AutoTokenizer.from_pretrained(path)
        model = AutoModelForSequenceClassification.from_pretrained(path)
        id2label = model.config.id2label
        self.labels = [id2label[i] for i in range(len(id2label))]
        if self.device == "cuda":
            model = model.half()
        self.model = model.to(self.device).eval()
        meta_file = path / "model_version.json"
        meta = json.loads(meta_file.read_text()) if meta_file.exists() else {}
        self.model_version = meta.get("version", path.name)
        # Fitted on the validation split at training time (1.0 for older weights).
        self.temperature = float(meta.get("temperature") or 1.0)
        self.ready = True
        self._warmup()
        logger.info("Loaded model %s on %s", self.model_version, self.device)

    def _count_tokens(self, text: str) -> int:
        return len(self.tokenizer.tokenize(text))

    def _warmup(self) -> None:
        self._score(["Traceback (most recent call last):\nValueError: warmup"])

    def windows(self, logs: list[str]) -> list[Window]:
        return [window(log, count_tokens=self._count_tokens) for log in logs]

    def _score(self, logs: list[str]) -> list[_Scored]:
        torch = self._torch
        wins = self.windows(logs)
        texts = [w.text or "(empty log)" for w in wins]
        batch = self.tokenizer(
            texts, truncation=True, max_length=MAX_LENGTH, padding=True, return_tensors="pt"
        ).to(self.device)
        with torch.inference_mode():
            logits = self.model(**batch).logits.float()
            probs = torch.softmax(logits / self.temperature, dim=-1).cpu().tolist()
        return [_Scored(w, p) for w, p in zip(wins, probs, strict=True)]

    def predict(self, logs: list[str], top_k: int = 3) -> dict[str, Any]:
        if not self.ready:
            raise RuntimeError("model not ready")
        started = time.perf_counter()
        logs = [log for log in logs if log and log.strip()]
        if not logs:
            return self._result(None, top_k, started)
        scored = self._score(logs)
        # Highest-confidence non-UNKNOWN window wins; all UNKNOWN -> the most confident one.
        unknown = self.labels.index("UNKNOWN")
        known = [s for s in scored if s.best != unknown]
        chosen = max(known or scored, key=lambda s: s.probs[s.best])
        return self._result(chosen, top_k, started)

    def _result(self, chosen: _Scored | None, top_k: int, started: float) -> dict[str, Any]:
        if self.device == "cuda":
            self._torch.cuda.synchronize()
        elapsed = round((time.perf_counter() - started) * 1000, 2)
        if chosen is None:
            return {
                "category": "UNKNOWN",
                "confidence": 0.0,
                "top_predictions": [],
                "model_version": self.model_version,
                "signal_line": None,
                "window": "",
                "device": self.device,
                "inference_ms": elapsed,
            }
        order = sorted(range(len(chosen.probs)), key=lambda i: chosen.probs[i], reverse=True)
        return {
            "category": self.labels[order[0]],
            "confidence": round(chosen.probs[order[0]], 4),
            "top_predictions": [
                {"category": self.labels[i], "score": round(chosen.probs[i], 4)}
                for i in order[:top_k]
            ],
            "model_version": self.model_version,
            "signal_line": chosen.window.signal_line,
            "window": chosen.window.text,
            "device": self.device,
            "inference_ms": elapsed,
        }

    def vram_used_mb(self) -> float | None:
        if self.device != "cuda":
            return None
        return round(self._torch.cuda.memory_allocated() / 1024 / 1024, 1)
