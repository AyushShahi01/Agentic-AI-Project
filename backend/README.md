# Backend

FastAPI control plane for Agentic Data Automation (see `../plan0.md`).

## Setup

```powershell
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements-dev.txt
copy .env.example .env      # then set JWT_SECRET_KEY, ENCRYPTION_KEY, ADMIN_PASSWORD
alembic upgrade head
```

The first start creates an admin from `ADMIN_EMAIL` / `ADMIN_PASSWORD` if no users exist.

## Run

```powershell
uvicorn app.main:app --reload
```

- Liveness: `GET http://127.0.0.1:8000/api/v1/health/live`
- API docs (development only): `http://127.0.0.1:8000/docs`

## Test & lint

```powershell
pytest                       # SQLite; PostgreSQL migration test runs when TEST_POSTGRES_URL is set
ruff check app tests
```

## Layout

| Path | Purpose |
|---|---|
| `app/api/v1` | HTTP routes (thin; delegate to services) |
| `app/core` | Settings, security (JWT/Argon2), secret encryption, DI, errors, rate limiting |
| `app/db`, `app/models` | SQLAlchemy engine/session, ORM models |
| `app/schemas` | Pydantic request/response models |
| `app/services` | Business logic + audit logging |
| `app/orchestration/airflow` | Airflow 2.x/3.x REST adapter, mock adapter, factory |
| `app/detection` | Detection rules, severity, evidence scrubbing (pure) + lease-guarded scheduler |
| `app/diagnosis` | Log classifier: failure category from task logs (pure) |
| `app/automation` | Workflow graph catalog + validation, policy, templates (pure) |
| `alembic/` | Migrations (`render_as_batch=True` for SQLite) |

## Detection

With `DETECTION_ENABLED=true` (default), the server polls monitored DAGs every
`DETECTION_INTERVAL_SECONDS` and opens incidents for failed runs and missed freshness SLAs
(set `SLA (min)` per DAG in the UI). Operators can also trigger a cycle with
`POST /api/v1/detection/run` or the "Run detection now" button. Only one worker polls at a time
(DB lease in `detection_leases`).

After pulling Plan 1, run `alembic upgrade head` to create the incident tables.

## Connection monitor

With `AIRFLOW_MONITOR_ENABLED=true` (default), the server lists DAGs from every active
connection every `AIRFLOW_MONITOR_INTERVAL_SECONDS` (30s). Success refreshes the DAG list and
marks the connection `HEALTHY`; failure stores the reason (`UNREACHABLE`, `UNAUTHORIZED`, ...).
Each status change is written to the audit log as `airflow_conn.status_changed`.

The API reports `is_live` on every connection: `HEALTHY` and checked within two monitor
intervals. The UI only shows a connection's DAGs while it is live; otherwise it shows
"Connection lost" / "Airflow not connected". Operators can force a check with
`POST /api/v1/airflow/connections/{id}/refresh` ("Retry now"). Only one worker polls
(`connections` lease in `detection_leases`).

## Automation (Plan 2)

Incidents trigger **workflows**: graphs of typed steps (trigger → filter → diagnose → approval →
Airflow action → verify → update incident / notify). Six built-in workflows are added, disabled,
on first start. Turn them on under *Automation → Workflows* (Admin), ideally in **Dry run** first.

- Runs are queued by detection and advanced right after each detection cycle (and after an
  approval decision, or `POST /api/v1/automation/tick`).
- Safety: any Airflow change on a `PROD` connection needs a human approval; at most
  `AUTOMATION_MAX_ACTIONS_PER_DAG_PER_DAY` automated actions per DAG; `AUTOMATION_FORCE_DRY_RUN=true`
  turns every workflow into a simulation. Everything is audited.
- Mock connections simulate retries: `partner_api_sync` fails with a transient HTTP 503 (a retry
  fixes it), `orders_pipeline` fails with bad data (a retry fails again).

After pulling Plan 2, run `alembic upgrade head` to create the automation tables.

## Local PostgreSQL / Airflow

```powershell
docker compose up -d postgres airflow3     # from the repo root
$env:DATABASE_URL = "postgresql+psycopg://postgres:postgres@localhost:5432/agentic_ai"
alembic upgrade head
```

Register Airflow in the UI with base URL `http://localhost:8080`, user `admin` / `admin`.
