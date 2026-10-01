"""Loading labeled JSONL, template-aware splitting and windowing for training."""

import hashlib
import json
import random
from collections.abc import Callable, Iterable
from pathlib import Path

from app.preprocess import window
from app.schemas import CATEGORIES

DATA = Path(__file__).resolve().parents[1] / "data"


def read_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line]


def data_hash(paths: Iterable[Path]) -> str:
    digest = hashlib.sha256()
    for path in paths:
        if path.exists():
            digest.update(path.read_bytes())
    return digest.hexdigest()[:16]


def split_by_template(
    rows: list[dict], val_fraction: float = 0.15, seed: int = 13
) -> tuple[list[dict], list[dict]]:
    """Hold out whole templates per category, so validation never sees a training template."""
    rng = random.Random(seed)
    by_cat: dict[str, set[str]] = {}
    for row in rows:
        by_cat.setdefault(row["label"], set()).add(row.get("template_id", row["id"]))
    held: set[str] = set()
    for templates in by_cat.values():
        ordered = sorted(templates)
        rng.shuffle(ordered)
        held.update(ordered[: max(1, round(len(ordered) * val_fraction))])
    train = [r for r in rows if r.get("template_id", r["id"]) not in held]
    val = [r for r in rows if r.get("template_id", r["id"]) in held]
    return train, val


def real_training_rows(eval_ids: set[str]) -> list[dict]:
    """Labeled exported logs that are NOT in the evaluation set."""
    rows = []
    for row in read_jsonl(DATA / "exported_task_logs.jsonl"):
        label = row.get("label") or row.get("operator_category")
        if not label or label not in CATEGORIES or not row.get("text"):
            continue
        rid = f"exported-{row['evidence_id']}"
        if rid in eval_ids:
            continue
        rows.append({"id": rid, "label": label, "template_id": rid, "text": row["text"]})
    return rows


def encode(rows: list[dict], count_tokens: Callable[[str], int]) -> tuple[list[str], list[int]]:
    texts = [window(r["text"], count_tokens=count_tokens).text or "(empty log)" for r in rows]
    labels = [CATEGORIES.index(r["label"]) for r in rows]
    return texts, labels
