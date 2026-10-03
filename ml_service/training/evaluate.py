"""Evaluate regex alone, model alone and the hybrid rule on the real eval set; apply the gate.

    python -m training.evaluate [--version <weights dir>] [--threshold 0.85]

Reports per-category precision/recall/F1 for each approach, a hybrid sweep over several
thresholds T, and the go/no-go gate (aiplan1.md, Phase 1):
  (a) hybrid overall accuracy >= regex accuracy,
  (b) hybrid correctly classifies >= 50% of the logs regex marks UNKNOWN,
  (c) at the chosen T, precision of model-driven overrides >= 0.9.
Synthetic validation metrics (from training) are shown but not gating. Results are written to
``data/eval_report.json`` and into the weights' ``model_version.json`` under ``real_eval``.
"""

import argparse
import importlib.util
import json
import sys
from pathlib import Path

from sklearn.metrics import classification_report

from app.config import get_settings
from app.model import Predictor, find_weights
from app.schemas import CATEGORIES
from training.dataset import DATA, read_jsonl

ROOT = Path(__file__).resolve().parents[2]
WEAK = {"UNKNOWN", "CODE_BUG"}
THRESHOLDS = (0.5, 0.6, 0.7, 0.8, 0.85, 0.9, 0.95)


def load_regex_classifier():
    """The backend's pure regex classifier, loaded by path (both packages are named `app`)."""
    path = ROOT / "backend" / "app" / "diagnosis" / "log_classifier.py"
    spec = importlib.util.spec_from_file_location("backend_log_classifier", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def hybrid(regex_cat: str, model_cat: str | None, conf: float, threshold: float) -> tuple[str, bool]:
    """Same rule as backend/app/diagnosis/hybrid.py. Returns (category, decided_by_model)."""
    if regex_cat not in WEAK or model_cat is None:
        return regex_cat, False
    if conf >= threshold and model_cat != "UNKNOWN":
        return model_cat, True
    return regex_cat, False


def accuracy(gold: list[str], pred: list[str]) -> float:
    return sum(g == p for g, p in zip(gold, pred, strict=True)) / max(1, len(gold))


def evaluate(rows: list[dict], regex: list[str], model: list[tuple[str, float]], t: float) -> dict:
    gold = [r["label"] for r in rows]
    decisions = [hybrid(rc, mc, conf, t) for rc, (mc, conf) in zip(regex, model, strict=True)]
    hyb = [d[0] for d in decisions]
    overrides = [i for i, d in enumerate(decisions) if d[1]]
    regex_unknown = [i for i, rc in enumerate(regex) if rc == "UNKNOWN"]
    return {
        "threshold": t,
        "hybrid_accuracy": round(accuracy(gold, hyb), 4),
        "regex_unknown_total": len(regex_unknown),
        "regex_unknown_correct": sum(hyb[i] == gold[i] for i in regex_unknown),
        "regex_unknown_rate": round(
            sum(hyb[i] == gold[i] for i in regex_unknown) / max(1, len(regex_unknown)), 4
        ),
        "model_overrides": len(overrides),
        "override_precision": round(
            sum(hyb[i] == gold[i] for i in overrides) / len(overrides), 4
        ) if overrides else None,
        "predictions": hyb,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--version", default=None)
    parser.add_argument("--threshold", type=float, default=0.85)
    parser.add_argument("--eval", default=str(DATA / "eval_real.jsonl"))
    args = parser.parse_args()

    rows = read_jsonl(Path(args.eval))
    if not rows:
        raise SystemExit(f"No eval data at {args.eval}; run python -m training.build_eval_set")
    gold = [r["label"] for r in rows]

    lc = load_regex_classifier()
    regex = [lc.classify(r["text"]).category.value for r in rows]

    settings = get_settings()
    predictor = Predictor(settings.weights_dir, args.version)
    if not predictor.ready:
        raise SystemExit("No trained weights; run python -m training.train first")
    model: list[tuple[str, float]] = []
    for r in rows:
        p = predictor.predict([r["text"]])
        model.append((p["category"], p["confidence"]))
    model_pred = [m[0] for m in model]

    labels = list(CATEGORIES)
    print(f"Eval set: {len(rows)} logs ({sum(r.get('origin') == 'exported' for r in rows)} "
          f"exported real, rest curated); model {predictor.model_version} on {predictor.device}\n")
    print("== Regex alone ==")
    print(classification_report(gold, regex, labels=labels, zero_division=0, digits=3))
    print("== Model alone ==")
    print(classification_report(gold, model_pred, labels=labels, zero_division=0, digits=3))

    sweep = [evaluate(rows, regex, model, t) for t in THRESHOLDS]
    chosen = evaluate(rows, regex, model, args.threshold)
    print(f"== Hybrid rule at T={args.threshold} ==")
    print(classification_report(gold, chosen["predictions"], labels=labels, zero_division=0,
                                digits=3))

    regex_acc = accuracy(gold, regex)
    print("== Hybrid sweep ==")
    print(f"{'T':>5} {'hybrid_acc':>10} {'regexUNK_ok':>12} {'overrides':>9} {'ovr_prec':>8}")
    for s in sweep:
        prec = "-" if s["override_precision"] is None else f"{s['override_precision']:.3f}"
        print(f"{s['threshold']:>5} {s['hybrid_accuracy']:>10.3f} "
              f"{s['regex_unknown_correct']:>5}/{s['regex_unknown_total']:<6} "
              f"{s['model_overrides']:>9} {prec:>8}")

    weak_idx = [i for i, rc in enumerate(regex) if rc in WEAK]
    gate = {
        "a_hybrid_accuracy_ge_regex": chosen["hybrid_accuracy"] >= regex_acc,
        "b_regex_unknown_correct_ge_50pct": chosen["regex_unknown_rate"] >= 0.5,
        "c_override_precision_ge_0_9": (chosen["override_precision"] or 0) >= 0.9,
    }
    gate["go"] = all(gate.values())
    summary = {
        "eval_size": len(rows),
        "threshold": args.threshold,
        "regex_accuracy": round(regex_acc, 4),
        "model_accuracy": round(accuracy(gold, model_pred), 4),
        "hybrid_accuracy": chosen["hybrid_accuracy"],
        "regex_weak_logs": len(weak_idx),
        "regex_weak_correct_regex": sum(regex[i] == gold[i] for i in weak_idx),
        "regex_weak_correct_hybrid": sum(chosen["predictions"][i] == gold[i] for i in weak_idx),
        "regex_unknown_total": chosen["regex_unknown_total"],
        "regex_unknown_correct_hybrid": chosen["regex_unknown_correct"],
        "model_overrides": chosen["model_overrides"],
        "override_precision": chosen["override_precision"],
        "sweep": [{k: v for k, v in s.items() if k != "predictions"} for s in sweep],
        "gate": gate,
    }
    print("\n== Summary ==")
    print(json.dumps({k: v for k, v in summary.items() if k != "sweep"}, indent=2))
    print("\nGO" if gate["go"] else "\nNO-GO")

    (DATA / "eval_report.json").write_text(json.dumps(
        {**summary, "model_version": predictor.model_version,
         "rows": [{"id": r["id"], "label": r["label"], "regex": rc, "model": m[0],
                   "model_confidence": m[1], "hybrid": h}
                  for r, rc, m, h in zip(rows, regex, model, chosen["predictions"], strict=True)]},
        indent=2))
    weights = find_weights(settings.weights_dir, args.version)
    meta_file = weights / "model_version.json"
    meta = json.loads(meta_file.read_text()) if meta_file.exists() else {}
    meta["real_eval"] = summary
    meta_file.write_text(json.dumps(meta, indent=2))


if __name__ == "__main__":
    main()
