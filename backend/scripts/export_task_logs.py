"""Export stored TASK_LOG evidence as JSONL for labeling and model training/evaluation.

    python scripts/export_task_logs.py [--out ../ml_service/data/exported_task_logs.jsonl]
                                       [--only-weak] [--limit N]

One row per log: the (already secret-scrubbed) text, the regex result, the incident's stored
diagnosis and, when an operator corrected it, ``operator_category``. ``label`` is left empty for a
human to fill in (``ml_service/training/build_eval_set.py`` only uses rows with a label or an
operator correction). ``--only-weak`` keeps logs regex marks UNKNOWN or CODE_BUG — the ones worth
labeling first.
"""

import argparse
import json
import sys
from pathlib import Path

BACKEND = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND))

from sqlalchemy import select  # noqa: E402

from app.db.session import SessionLocal  # noqa: E402
from app.detection.types import EvidenceKind  # noqa: E402
from app.diagnosis.log_classifier import FailureCategory, classify  # noqa: E402
from app.models.incident import Incident, IncidentEvidence  # noqa: E402

WEAK = {FailureCategory.UNKNOWN, FailureCategory.CODE_BUG}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument(
        "--out",
        default=str(BACKEND.parent / "ml_service" / "data" / "exported_task_logs.jsonl"),
    )
    parser.add_argument("--only-weak", action="store_true", help="regex UNKNOWN/CODE_BUG only")
    parser.add_argument("--limit", type=int, default=0)
    args = parser.parse_args()

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    written = 0
    with SessionLocal() as db, out.open("w", encoding="utf-8") as fh:
        rows = db.execute(
            select(IncidentEvidence, Incident)
            .join(Incident, Incident.id == IncidentEvidence.incident_id)
            .where(
                IncidentEvidence.kind == EvidenceKind.TASK_LOG,
                IncidentEvidence.content.is_not(None),
            )
            .order_by(IncidentEvidence.collected_at.desc())
        )
        for evidence, incident in rows:
            regex = classify(evidence.content)
            if args.only_weak and regex.category not in WEAK:
                continue
            stored = incident.diagnosis or {}
            operator = stored.get("category") if stored.get("source") == "operator" else None
            fh.write(
                json.dumps(
                    {
                        "evidence_id": str(evidence.id),
                        "incident_id": str(incident.id),
                        "dag_id": incident.dag_id,
                        "source": evidence.source,
                        "collected_at": evidence.collected_at.isoformat(),
                        "regex_category": regex.category.value,
                        "regex_rule": regex.rule,
                        "stored_category": stored.get("category"),
                        "stored_source": stored.get("source"),
                        "operator_category": operator,
                        "label": operator,  # fill in by hand; operator corrections pre-filled
                        "text": evidence.content,
                    }
                )
                + "\n"
            )
            written += 1
            if args.limit and written >= args.limit:
                break
    print(f"wrote {written} logs to {out}")


if __name__ == "__main__":
    main()
