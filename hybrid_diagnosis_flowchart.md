# Hybrid AI Diagnosis System Flowchart

Here is the visual hand-drawn architecture and execution flow for [AI Plan 1](file:///d:/projects/agenticAi/aiplan1.md):

![Hybrid AI Diagnosis Flowchart](C:/Users/ayush/.gemini/antigravity-ide/brain/806aa927-bfef-48ab-90e7-5ae5c93fad41/aiplan1_flowchart_1790830934686.jpg)

---

## Decision Logic Breakdown

```mermaid
flowchart TD
    Log["Airflow Task Failure / Log Ingestion"] --> Stage1["Stage 1: Deterministic Regex Classifier\n(Rules: OOM, Auth, Integrity, Schema, etc.)"]
    
    Stage1 --> Check{"Category in\n{UNKNOWN, CODE_BUG}?"}
    
    Check -- "No (Specific Rule Matched)" --> SaveRegex["Save Regex Diagnosis\nsource = 'regex'\nconfidence = 0.9–0.95"]
    
    Check -- "Yes (Weak Regex Catch-all)" --> CB["ml_client\n(Circuit Breaker + Secret Redaction)"]
    
    CB --> MLService["GPU ML Service :8001\n(DistilBERT 512-Token Windowing)"]
    
    MLService --> ConfCheck{"Model Confidence\n>= 85% (T)?"}
    
    ConfCheck -- "Yes" --> SaveModel["Save Model Diagnosis\nsource = 'model'\ncategory = model.category"]
    
    ConfCheck -- "No / Fallback" --> SaveSuggestion["Save Regex Category\nsuggestion = model.category\nsource = 'regex'"]
    
    SaveRegex --> DB[("Database: Incident.diagnosis JSON\n+ 'diagnosed' event")]
    SaveModel --> DB
    SaveSuggestion --> DB
    
    DB --> Workflows["Automation Workflows\n(Evaluates final category & retryable)"]
    DB --> UI["Incident Detail UI\n(DiagnosisCard, badges, CSS probability bars)"]
    
    UI --> Override["Operator Correction\n(PUT /incidents/{id}/diagnosis)"]
    Override -.->|"Locks source = 'operator'\nFeeds export_task_logs.py"| DB
```

---

## Step-by-Step Flow

1. **Detection & Evidence Attachment**:
   - When a task fails, [detection_service.py](file:///d:/projects/agenticAi/backend/app/services/detection_service.py) attaches `TASK_LOG` evidence and calls `diagnose_and_store()`.
2. **Stage 1 (Regex Classifier)**:
   - [log_classifier.py](file:///d:/projects/agenticAi/backend/app/diagnosis/log_classifier.py) runs high-confidence deterministic rules first.
   - If a specific pattern matches (`AUTH`, `DATA_INTEGRITY`, `SCHEMA`, `RESOURCE`, `TIMEOUT`, `UPSTREAM_MISSING`), diagnosis completes with `source="regex"`. No ML invocation is made.
3. **Stage 2 (ML Service via Circuit Breaker)**:
   - If regex returns `UNKNOWN` or generic `CODE_BUG`, `ml_client` redacts sensitive credentials and calls `POST http://127.0.0.1:8001/v1/classify`.
   - The GPU service extracts the innermost traceback/error window (up to 512 tokens) and runs fine-tuned **DistilBERT** sequence classification.
4. **Hybrid Decision Rule**:
   - **Confidence $\ge 85\%$**: Model overrides generic regex output (`source="model"`).
   - **Confidence $< 85\%$ or ML unavailable**: Falls back to regex category, attaching model prediction as a display-only `suggestion`.
5. **Persistence & Consumers**:
   - The diagnosis is persisted directly on `Incident.diagnosis` JSON column (offline persistence, avoiding model calls on `GET /incidents/{id}`).
   - Workflows read the stored category; operators can manually override and export corrections for ongoing fine-tuning.
