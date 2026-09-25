# Agentic Data Automation

## Project Goal

Build an AI-powered operations platform for monitoring data pipelines and automatically helping recover from failures and data-quality incidents.

The platform should monitor Airflow and other data workflows, detect operational and data-quality problems, investigate likely root causes, recommend or execute safe remediation, and verify the result.

This is not only a CRUD application or a generic chatbot. It is an incident detection, diagnosis, approval, execution, and verification system for data-platform operations.

## Core Workflow

```text
Monitor pipelines and datasets
  -> Detect failures, anomalies, or quality violations
  -> Collect logs, metrics, metadata, and recent changes
  -> Classify the incident
  -> Diagnose the likely root cause
  -> Retrieve relevant historical incidents and documentation
  -> Plan remediation
  -> Apply safety and approval policies
  -> Execute the approved action
  -> Verify recovery and data quality
  -> Record the incident, evidence, outcome, and audit trail
```

## Data-Quality Monitoring

Data quality is a first-class concern alongside pipeline reliability. The system should monitor, where applicable:

- Freshness and delivery latency
- Completeness and missing records
- Null, blank, and duplicate rates
- Validity of values and formats
- Schema changes and schema drift
- Row counts and volume anomalies
- Distribution and statistical anomalies
- Referential integrity and consistency across datasets
- SLA violations
- Failed quality checks and rule thresholds

A quality incident should be treated as an operational incident with evidence, severity, ownership, diagnosis, remediation, and verification.

## Main Components

- `backend`: FastAPI control plane and API layer.
- `frontend`: React user interface for incidents, jobs, quality checks, approvals, and agent activity.
- `orchestration`: Airflow clients, workflow coordination, sensors, and reusable application workflows.
- `agents`: Graph-based agent workflow for monitoring, detection, diagnosis, planning, execution, and verification.
- `detection`: Anomaly detectors, log classifiers, SLA rules, schema-drift rules, and data-quality rules.
- `diagnosis`: Log analysis, stack-trace parsing, change analysis, and root-cause reasoning.
- `knowledge`: Incident history, logs, documentation, embeddings, retrieval, and reranking.
- `execution`: Controlled adapters for Airflow, shell commands, SQL, Kubernetes, and recovery actions.
- `monitoring`: Application health, metrics, structured logs, and operational observability.

## Safety Requirements

- Agents must not execute actions directly without policy checks.
- Potentially destructive or production-impacting actions require explicit approval unless a documented policy allows automation.
- Every action must have an audit record containing the request, evidence, policy decision, approver, execution result, and verification result.
- Execution adapters should be narrow, testable, and independently permissioned.
- Secrets must come from environment or secret-management configuration, never from committed source files.
- Dry-run and simulation modes should be available before enabling autonomous execution.

## Architecture Principles

- Keep business workflows independent of FastAPI, Airflow, and the React UI.
- Treat API endpoints, Airflow DAGs, agents, and CLI scripts as adapters around shared application services.
- Keep Airflow DAG files thin; reusable workflow logic belongs in the orchestration layer.
- Avoid circular dependencies between agents, diagnosis, knowledge, and execution.
- Prefer typed models for incidents, jobs, quality checks, evidence, plans, approvals, actions, and verification results.
- Make detection and remediation components deterministic and independently testable where possible.
- Preserve evidence and explainability for every agent decision.

## Initial MVP

1. Register and monitor a small set of Airflow jobs.
2. Detect task failures, SLA breaches, and basic data-quality failures.
3. Store incidents with logs, severity, status, and timestamps.
4. Provide a simple diagnosis using logs and recent job metadata.
5. Recommend retry or clear-task actions.
6. Require approval before execution.
7. Execute one safe recovery action.
8. Verify job status and selected data-quality checks.
9. Display incidents, quality results, approvals, and outcomes in the React frontend.

## Development Guidance

When changing the codebase, preserve the end goal above and prefer a small vertical slice over disconnected infrastructure. A useful feature should connect detection, evidence, diagnosis, policy, action, verification, and user-visible status where practical.

Do not add advanced ML, vector search, autonomous execution, or broad provider integrations before the basic incident lifecycle works reliably. Use clear interfaces so those capabilities can be added later.

Every new feature should answer:

- What failure or quality problem does it detect or resolve?
- What evidence does it use?
- What decision or action does it produce?
- What approval and safety rules apply?
- How is success verified?
- How is the result exposed to operators and recorded for audit?

## Current Workspace Status

- Plan 0 (`plan0.md`): auth & RBAC, persistence with Alembic, audit log, version-aware Airflow adapter (2.x/3.x + mock), monitored-DAG registry.
- Plan 1 (`plan1.md`): failure and freshness-SLA detection for monitored DAGs, deduplicated incidents with evidence (runs, task instances, scrubbed log tails), auto-resolve on recovery, a lease-guarded background detector, and an Incidents UI.
- `backend`: FastAPI app in `backend/app` (`uvicorn app.main:app`), tests in `backend/tests`.
- Plan 2 (`plan2.md`): automation workflows. Deterministic log diagnosis; workflows stored as a graph of typed nodes (trigger, filter, classify, approval, Airflow action, verify, incident update, notify); a policy layer (PROD actions need an approval, daily per-DAG limit, dry run); six built-in workflows; Approvals, Runs, Workflows and Notifications UI.
- Plan 3 (`plan3.md`): visual canvas. Drag-and-drop workflow editor (React Flow; palette and settings forms generated from the node catalog; server validation shown on the blocks), a pipeline canvas (Airflow connection → DAG → Failure / SLA monitor blocks → the automations they feed), and run replay on the canvas.
- `frontend`: React + Vite app (dashboard, incidents, pipelines canvas, automation and workflow editor, Airflow connections, monitored DAGs, users).
- Next: Plan 4 — data-quality monitors (row count, null rate, freshness column, schema drift) as new monitor blocks, with a database-source block.
