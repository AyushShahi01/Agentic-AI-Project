"""Fine-tune distilbert-base-uncased on synthetic (+ labeled real) task logs.

    python -m training.train [--epochs 3] [--batch-size 16] [--lr 5e-5] [--label-smoothing 0.1]

Splits by template id (no leakage), trains with HF Trainer (AdamW, fp16 on CUDA) and saves to
``weights/<yyyymmdd-hhmm>/`` with ``model_version.json`` (base model, data hash, metrics).

Confidence matters as much as accuracy (the hybrid rule only lets the model act above a threshold),
so training uses label smoothing and then fits a softmax *temperature* on the held-out-template
validation split (minimising NLL). The temperature is stored in ``model_version.json`` and applied
by ``app.model.Predictor``; ``val_ece`` before/after shows how calibrated the scores are.
Labeled real logs from ``data/exported_task_logs.jsonl`` that are not in the eval set are added
to the training split.
"""

import argparse
import json
import platform
import shutil
from datetime import UTC, datetime
from pathlib import Path

import numpy as np
import torch
from sklearn.metrics import accuracy_score, f1_score
from torch.utils.data import Dataset
from transformers import (
    AutoModelForSequenceClassification,
    AutoTokenizer,
    DataCollatorWithPadding,
    Trainer,
    TrainingArguments,
)

from app.schemas import CATEGORIES
from app.model import ece
from training.dataset import DATA, data_hash, encode, read_jsonl, real_training_rows, split_by_template

BASE_MODEL = "distilbert-base-uncased"
WEIGHTS = Path(__file__).resolve().parents[1] / "weights"


class _Encoded(Dataset):
    def __init__(self, tokenizer, texts: list[str], labels: list[int]):
        self.enc = tokenizer(texts, truncation=True, max_length=512)
        self.labels = labels

    def __len__(self) -> int:
        return len(self.labels)

    def __getitem__(self, i: int) -> dict:
        item = {k: v[i] for k, v in self.enc.items()}
        item["labels"] = self.labels[i]
        return item


def _metrics(pred) -> dict[str, float]:
    preds = np.argmax(pred.predictions, axis=-1)
    return {
        "accuracy": float(accuracy_score(pred.label_ids, preds)),
        "macro_f1": float(f1_score(pred.label_ids, preds, average="macro")),
    }


def fit_temperature(logits: np.ndarray, labels: np.ndarray) -> float:
    """Single temperature T minimising validation NLL of softmax(logits / T)."""
    z = torch.tensor(logits, dtype=torch.float32)
    y = torch.tensor(labels, dtype=torch.long)
    log_t = torch.zeros(1, requires_grad=True)  # optimise log T so T stays positive
    opt = torch.optim.LBFGS([log_t], lr=0.1, max_iter=200)

    def closure():
        opt.zero_grad()
        loss = torch.nn.functional.cross_entropy(z / log_t.exp(), y)
        loss.backward()
        return loss

    opt.step(closure)
    return float(log_t.exp().clamp(0.05, 20.0))


def _probs(logits: np.ndarray, temperature: float = 1.0) -> np.ndarray:
    return torch.softmax(torch.tensor(logits, dtype=torch.float32) / temperature, dim=-1).numpy()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--synthetic", default=str(DATA / "synthetic.jsonl"))
    parser.add_argument("--epochs", type=float, default=3)
    parser.add_argument("--batch-size", type=int, default=16)
    parser.add_argument("--lr", type=float, default=5e-5)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--label-smoothing", type=float, default=0.1)
    args = parser.parse_args()

    synthetic = read_jsonl(Path(args.synthetic))
    if not synthetic:
        raise SystemExit(f"No data at {args.synthetic}; run python -m training.generate_synthetic")
    eval_ids = {r["id"] for r in read_jsonl(DATA / "eval_real.jsonl")}
    train_rows, val_rows = split_by_template(synthetic)
    real_rows = real_training_rows(eval_ids)
    train_rows += real_rows
    print(f"train={len(train_rows)} (real={len(real_rows)}) val={len(val_rows)} (held-out templates)")

    cuda = torch.cuda.is_available()
    tokenizer = AutoTokenizer.from_pretrained(BASE_MODEL)

    def count(text: str) -> int:
        return len(tokenizer.tokenize(text))

    train_ds = _Encoded(tokenizer, *encode(train_rows, count))
    val_ds = _Encoded(tokenizer, *encode(val_rows, count))
    model = AutoModelForSequenceClassification.from_pretrained(
        BASE_MODEL,
        num_labels=len(CATEGORIES),
        id2label=dict(enumerate(CATEGORIES)),
        label2id={c: i for i, c in enumerate(CATEGORIES)},
    )

    version = datetime.now(UTC).strftime("%Y%m%d-%H%M")
    out_dir = WEIGHTS / version
    work_dir = WEIGHTS / f".run-{version}"
    training_args = TrainingArguments(
        output_dir=str(work_dir),
        num_train_epochs=args.epochs,
        per_device_train_batch_size=args.batch_size,
        per_device_eval_batch_size=args.batch_size * 2,
        learning_rate=args.lr,
        weight_decay=0.01,
        label_smoothing_factor=args.label_smoothing,
        warmup_steps=int(len(train_ds) / args.batch_size * args.epochs * 0.06),
        optim="adamw_torch",
        fp16=cuda,
        eval_strategy="epoch",
        save_strategy="epoch",
        save_total_limit=1,
        load_best_model_at_end=True,
        metric_for_best_model="macro_f1",
        logging_steps=50,
        seed=args.seed,
        report_to=[],
        dataloader_num_workers=0,
    )
    trainer = Trainer(
        model=model,
        args=training_args,
        train_dataset=train_ds,
        eval_dataset=val_ds,
        data_collator=DataCollatorWithPadding(tokenizer),
        compute_metrics=_metrics,
    )
    started = datetime.now(UTC)
    trainer.train()
    metrics = trainer.evaluate()
    val = trainer.predict(val_ds)
    temperature = fit_temperature(val.predictions, val.label_ids)
    calibration = {
        "temperature": round(temperature, 4),
        "val_ece_raw": round(ece(_probs(val.predictions), val.label_ids), 4),
        "val_ece_calibrated": round(ece(_probs(val.predictions, temperature), val.label_ids), 4),
    }
    print(f"calibration: {calibration}")
    trainer.save_model(str(out_dir))
    tokenizer.save_pretrained(str(out_dir))

    meta = {
        "version": version,
        "base_model": BASE_MODEL,
        "labels": list(CATEGORIES),
        "data_hash": data_hash([Path(args.synthetic), DATA / "exported_task_logs.jsonl"]),
        "train_size": len(train_rows),
        "real_train_size": len(real_rows),
        "val_size": len(val_rows),
        "split": "by template id (15% of templates per category held out)",
        "epochs": args.epochs,
        "batch_size": args.batch_size,
        "learning_rate": args.lr,
        "label_smoothing": args.label_smoothing,
        **calibration,
        "fp16": cuda,
        "device": torch.cuda.get_device_name(0) if cuda else platform.processor() or "cpu",
        "trained_at": started.isoformat(),
        "train_seconds": round((datetime.now(UTC) - started).total_seconds(), 1),
        "synthetic_val_metrics": {k: round(v, 4) for k, v in metrics.items()
                                  if isinstance(v, float)},
    }
    (out_dir / "model_version.json").write_text(json.dumps(meta, indent=2))
    shutil.rmtree(work_dir, ignore_errors=True)
    print(json.dumps(meta, indent=2))
    print(f"saved to {out_dir}")


if __name__ == "__main__":
    main()
