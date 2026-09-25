# Plan 4: Production Hardening

## 1. Overview & Objective

Plans 0–3 built detection, automation workflows and the visual canvas. So far they have only been checked against fakes: a scripted fake Airflow, the in-process mock, SQLite, and a frontend build without a browser. Before we add more capability (Plan 5: data quality), the core loop must be **proven on the real stack**, because a wrong Airflow call in PROD destroys trust in automation.

Target outcome: **a real Airflow 3 task failure is detected, diagnosed, retried by the `retry-transient` workflow, verified and resolved end to end, with the backend running on PostgreSQL; the whole backend test suite passes on PostgreSQL; the canvas UI is exercised in a real browser by Playwright; and automation advances on its own fast loop instead of waiting for the detection interval.**

Scope:
1. **Live Airflow end-to-end test** with test DAGs mounted into the docker-compose Airflow.
2. **PostgreSQL**: run the whole backend suite (not only migrations) against PostgreSQL.
3. **Separate automation loop** with its own lease and interval.
4. **Browser smoke tests** (Playwright) for login, pipelines canvas, workflow editor and run replay.
5. **CI definition** that runs all of the above.

### Out of Scope
- New features (data quality, alerting, insight): Plans 5–7.
- Load/performance testing and high availability.

### Key Decisions
| Decision | Choice | Rationale |
|---|---|---|
| Test DAGs | Mounted from `deploy/airflow/dags/` into both Airflow containers; examples off | Deterministic scenarios instead of example DAGs. |
| "Fails once" DAG | Fails with a network error until a marker file exists, then succeeds | A real clear/rerun genuinely fixes it, the same shape as the mock `partner_api_sync`. |
| Live test gating | `pytest -m live_airflow`, skipped unless `LIVE_AIRFLOW_URL` is set | The normal suite stays fast and offline. |
| PostgreSQL suite | `TEST_DATABASE_URL` makes `conftest.py` use that database (drop/create schema per test) | Catches dialect issues such as partial indexes, CHECK constraints, JSON and booleans. |
| Automation loop | Reuse `DetectionScheduler` (generalised: lease name, label) as a second loop, every `AUTOMATION_INTERVAL_SECONDS` (default 30) | One tested mechanism; run claims already make concurrent ticks safe. |
| Manual detection run | Still advances automation right after (immediate feedback in the UI) | The scheduled detection cycle no longer does; the automation loop does. |
| Browser tests | `@playwright/test` (Chromium); Playwright starts the backend (mock Airflow, pinned clock) and the Vite dev server | Real browser coverage of the canvas without Docker. |

---

## 2. Components

### 2.1. Test DAGs (`deploy/airflow/dags/`)
- `agentic_flaky_once`: manual schedule, `retries=0`. Task `fetch_partner_orders` raises `ConnectionError("503 Service Unavailable …")` unless `/tmp/agentic_flaky_marker` exists; on the failing attempt it creates the marker. Diagnosis: `TRANSIENT_NETWORK`. A retry fixes it.
- `agentic_bad_data`: manual schedule; always raises `UniqueViolation: duplicate key value violates unique constraint`. Diagnosis: `DATA_INTEGRITY`. Must **not** be retried.
- Written to import cleanly on both Airflow 2.10 and 3.1.

### 2.2. Live Airflow test (`backend/tests/live/test_live_airflow.py`)
The test runs against `LIVE_AIRFLOW_URL` (default user `admin`/`admin`), with a fresh database and in-process services:
1. Create a LIVE DEV connection. Probe it (health and version) and sync DAGs.
2. Monitor both test DAGs; enable `retry-transient` and `no-retry-data-code`.
3. Unpause both DAGs, remove the marker (via a first reset run), and trigger both through the adapter. Wait until both runs fail.
4. Run detection: 2 incidents open, with real task logs as evidence.
5. Tick automation until the runs are terminal (verify poll shortened to 5s):
   - `agentic_flaky_once` is cleared through the real API, the rerun succeeds, and the incident is `AUTO_REMEDIATED`.
   - `agentic_bad_data` is not retried; it gets a note and a notification.
6. Also exercise `set_dag_paused` and `get_dag` round trips.

### 2.3. PostgreSQL suite
- `conftest.py`: if `TEST_DATABASE_URL` is set, use it with `drop_all`/`create_all` per test; otherwise use in-memory SQLite as today.
- The existing migration round-trip test also runs (it reads `TEST_POSTGRES_URL`; accept either variable).

### 2.4. Automation loop
- `detection/scheduler.py`: the scheduler takes a `name` (for logs and the lease). `detection_service.try_acquire_lease` accepts a lease name.
- `app/main.py`: a second loop, `automation`, runs `automation_service.tick` every `AUTOMATION_INTERVAL_SECONDS` when `AUTOMATION_ENABLED`. The scheduled detection cycle only detects; `POST /detection/run` detects, then ticks.
- `GET /api/v1/automation/status`: enabled, interval, whether the loop is running, the last tick summary and the last error. Shown on the dashboard automation card.

### 2.5. Browser smoke tests (`frontend/e2e/`)
- `playwright.config.js` starts:
  - the backend: `backend/scripts/e2e_server.py`, a fresh SQLite database with the mock Airflow clock pinned so `partner_api_sync` has just failed
  - the Vite dev server
- Specs:
  - **Login**, and **pipelines canvas** shows the connection, DAG and monitor blocks. Attach an SLA monitor by drag and connect, and see it appear.
  - **Workflow editor**: create a workflow from the palette (drag, connect, configure), save, reload, and find the same layout. Invalid wiring shows problems.
  - **Run replay**: run detection with `retry-transient` enabled and open the run. The canvas shows the executed blocks.
- Screenshots on failure, plus one stored screenshot per page for review.

### 2.6. CI (`.github/workflows/ci.yml`)
- **Backend job:** a PostgreSQL service, then ruff and pytest (with `TEST_DATABASE_URL`).
- **Frontend job:** lint, build, `test:canvas`, Playwright.
- **Manual `live-airflow` job:** docker compose (Postgres + Airflow 3), then `pytest -m live_airflow`.

---

## 3. Implementation Steps

### Phase 1: Automation loop
- [ ] Generalise the scheduler and lease; add the automation loop, settings and status endpoint; dashboard card.
- **Tests**: the automation loop ticks due runs; the lease is exclusive per name; a scheduled detection cycle no longer ticks; the manual run still does.

### Phase 2: PostgreSQL
- [ ] `TEST_DATABASE_URL` support; run the full suite on docker PostgreSQL; fix what breaks.

### Phase 3: Live Airflow
- [ ] Test DAGs + compose mounts; live test; fix adapter mismatches found against Airflow 3.1 (and 2.10 if time permits).

### Phase 4: Browser tests
- [ ] Playwright setup, e2e server script, the three specs; fix UI bugs found.

### Phase 5: CI + docs
- [ ] CI workflow; README "Testing" section; `project.md` status.

---

## 4. Definition of Done
1. `pytest` passes on SQLite **and** on PostgreSQL; `ruff`, `oxlint`, build and `test:canvas` are clean.
2. `pytest -m live_airflow` passes against docker-compose Airflow 3.1: a real failure is auto-remediated and the bad-data failure is left alone.
3. Playwright specs pass in Chromium; screenshots of the pipelines canvas, the workflow editor and a run replay have been reviewed.
4. Automation advances within `AUTOMATION_INTERVAL_SECONDS` of a run becoming due, independent of detection.
