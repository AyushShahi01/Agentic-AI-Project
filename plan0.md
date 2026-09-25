# Plan 0: Foundational Architecture & Core Integrations

## 1. Overview & Objective

Plan 0 establishes the **minimum foundation** needed for the first incident-lifecycle slice described in `project.md` (MVP step 1: *"Register and monitor a small set of Airflow jobs"*). Per `project.md`, we prefer a small vertical slice over disconnected infrastructure, so this plan deliberately stops at: **a secured operator can register an Airflow connection, test it, sync its DAGs, and mark DAGs as monitored** — with every change audited.

Scope:

1. **Auth & RBAC (minimal)**: Admin-created accounts, JWT access/refresh tokens, three roles (Admin, Operator, Viewer).
2. **Persistence Layer**: SQLAlchemy 2.0 ORM, Alembic migrations (PostgreSQL + SQLite-compatible), audit-log entity.
3. **Airflow Orchestration Adapter**: Version-aware (Airflow 2.x `/api/v1` and 3.x `/api/v2`) REST client, per-connection mock adapter, encrypted credentials, health probes.
4. **Monitored DAG Registry**: Sync DAGs from a connection into the DB and toggle `is_monitored` — the bridge to Plan 1 (failure detection).
5. **Frontend Foundation**: React + Vite shell with login, Airflow connection management, and monitored-DAG selection.

### Out of Scope (deferred to later plans)
- `SERVICE_AGENT` role and machine authentication (API keys / client credentials) — designed when agents exist.
- Public self-signup, password reset, SSO.
- Incidents, Approvals, and Pipelines pages (not shown in the sidebar until they exist).
- Advanced design-system work (theming, glassmorphism). UI stays plain and functional.

### Key Decisions
| Decision | Choice | Rationale |
|---|---|---|
| Airflow versions | 2.x **and** 3.x via version detection | Airflow 3 removed `/api/v1`; many deployments are still on 2.x. |
| Signup | **Admin-only** (no public signup) | Platform will eventually execute remediation; open signup is unsafe. |
| Airflow client location | `backend/app/orchestration/airflow/` | Matches the `orchestration` layer defined in `project.md`. |
| Roles | `ADMIN`, `OPERATOR`, `VIEWER` | `SERVICE_AGENT` deferred until agents exist. |
| Sync vs async | Async HTTP (`httpx.AsyncClient`), sync DB sessions (`Session`) | Keeps ORM simple; FastAPI runs sync DB work in its threadpool. Services must not mix async DB access. |

---

## 2. Architecture & Directory Layout

```text
agenticAi/
├── backend/
│   ├── app/
│   │   ├── main.py                  # FastAPI app factory (replaces backend/main.py; old /health moves to /api/v1/health/live)
│   │   ├── api/
│   │   │   └── v1/
│   │   │       ├── auth.py          # /api/v1/auth (login, refresh, logout, me)
│   │   │       ├── users.py         # /api/v1/users (admin: create, list, update, deactivate)
│   │   │       ├── airflow.py       # /api/v1/airflow (connections, test, dag sync, monitored dags)
│   │   │       ├── health.py        # /api/v1/health/live (public), /api/v1/health (authed detail)
│   │   │       └── router.py        # Master API v1 router
│   │   ├── core/
│   │   │   ├── config.py            # Pydantic Settings + production safety checks
│   │   │   ├── security.py          # Password hashing (pwdlib/argon2), JWT (PyJWT), token types
│   │   │   ├── crypto.py            # Fernet encryption for stored Airflow secrets
│   │   │   ├── dependencies.py      # get_db, get_current_user (DB-backed), require_role(...)
│   │   │   ├── rate_limit.py        # Login attempt limiting
│   │   │   └── exceptions.py        # Domain exceptions + uniform error response shape
│   │   ├── db/
│   │   │   ├── session.py           # Engine factory (Postgres pooling vs SQLite settings)
│   │   │   ├── base.py              # DeclarativeBase + TimestampMixin
│   │   │   └── init_db.py           # Bootstrap admin from env (first run only)
│   │   ├── models/
│   │   │   ├── user.py              # User (role = enum column), RefreshToken
│   │   │   ├── airflow.py           # AirflowConnection, MonitoredDag
│   │   │   └── audit_log.py         # Append-only AuditLog
│   │   ├── schemas/
│   │   │   ├── user.py              # UserCreate, UserRead, UserUpdate, TokenPair
│   │   │   ├── airflow.py           # AirflowConnCreate/Update/Read, ConnTestResult, DagRead
│   │   │   └── health.py            # LivenessResponse, HealthStatusResponse
│   │   ├── orchestration/
│   │   │   └── airflow/
│   │   │       ├── base.py          # AirflowAdapter protocol + ConnectionStatus enum + DTOs
│   │   │       ├── client.py        # Live REST adapter (v1/v2 detection, auth strategies)
│   │   │       ├── mock.py          # Mock adapter (per-connection, blocked in production)
│   │   │       └── factory.py       # get_adapter(connection) -> live | mock
│   │   └── services/
│   │       ├── auth_service.py      # Login, refresh rotation, logout/revocation
│   │       ├── user_service.py      # User CRUD, deactivation
│   │       ├── airflow_service.py   # Connection CRUD, test, DAG sync, monitoring toggle
│   │       └── audit_service.py     # record(action, entity, details, actor)
│   ├── alembic/                     # Migrations (render_as_batch=True for SQLite)
│   ├── tests/                       # pytest suite (per-phase, see §4)
│   ├── alembic.ini
│   ├── requirements.txt
│   ├── requirements-dev.txt
│   └── .env.example
│
├── frontend/
│   ├── src/
│   │   ├── components/              # Button, Input, Modal, Badge, Card, StatusPill
│   │   ├── context/                 # AuthContext, ToastContext
│   │   ├── layouts/                 # AppLayout (Sidebar, Header), AuthLayout
│   │   ├── pages/
│   │   │   ├── auth/                # Login
│   │   │   ├── dashboard/           # Foundation status overview
│   │   │   └── settings/            # Airflow Connections, Monitored DAGs, Users (admin)
│   │   ├── services/                # apiClient (fetch/axios wrapper), authApi, airflowApi, healthApi
│   │   ├── types/                   # JSDoc typedefs
│   │   ├── App.jsx                  # Routes (react-router)
│   │   ├── index.css                # CSS variables & base styles
│   │   └── main.jsx
│   └── vite.config.js               # Dev proxy /api -> backend
│
├── docker-compose.yml               # PostgreSQL 16 + Airflow 3.x (optional profile: Airflow 2.x)
├── project.md
└── plan0.md                         # This blueprint document
```

---

## 3. Core Components Breakdown

### 3.1. Database Engine & Migration Setup
- **Target DB**: PostgreSQL (docker/production). SQLite supported for local dev and tests.
- **ORM**: SQLAlchemy 2.0 typed declarative (`Mapped`, `mapped_column`), `sqlalchemy.Uuid` and `JSON` types (portable across both DBs).
- **Driver**: `psycopg` 3 (`postgresql+psycopg://`).
- **Engine settings** (chosen by URL scheme in `db/session.py`):
  - PostgreSQL: `pool_size=10`, `max_overflow=20`, `pool_pre_ping=True`, `pool_recycle=1800`.
  - SQLite: `connect_args={"check_same_thread": False}`, no pool sizing, `PRAGMA foreign_keys=ON` on connect.
- **Migrations**: Alembic with `render_as_batch=True` (required for SQLite `ALTER`), autogenerate enabled, naming convention set on `MetaData` so constraint names are deterministic.

#### Database Schema Definitions

```mermaid
erDiagram
    USERS ||--o{ AUDIT_LOGS : "acts in"
    USERS ||--o{ REFRESH_TOKENS : owns
    USERS ||--o{ AIRFLOW_CONNECTIONS : creates
    AIRFLOW_CONNECTIONS ||--o{ MONITORED_DAGS : discovers
    USERS {
        uuid id PK
        string email UK
        string full_name
        string hashed_password
        enum role "ADMIN | OPERATOR | VIEWER"
        boolean is_active
        timestamp last_login_at
        timestamp created_at
        timestamp updated_at
    }
    REFRESH_TOKENS {
        uuid id PK "also the JWT jti"
        uuid user_id FK
        timestamp expires_at
        timestamp revoked_at "null = active"
        uuid replaced_by "rotation chain"
        timestamp created_at
    }
    AIRFLOW_CONNECTIONS {
        uuid id PK
        string name UK
        string environment "dev | staging | prod"
        string kind "LIVE | MOCK"
        string base_url "host root, no /api/vN suffix"
        string auth_type "BASIC | TOKEN | NONE"
        string username
        string encrypted_secret "password or token, Fernet"
        string api_version "v1 | v2, detected"
        string airflow_version "e.g. 3.1.0, detected"
        boolean is_active
        boolean is_default "partial unique index: only one TRUE"
        enum last_health_status
        string last_health_message
        int last_latency_ms
        timestamp last_checked_at
        uuid created_by FK
        timestamp created_at
        timestamp updated_at
    }
    MONITORED_DAGS {
        uuid id PK
        uuid connection_id FK
        string dag_id "UNIQUE(connection_id, dag_id)"
        string description
        string schedule_summary "timetable_summary / schedule_interval"
        boolean is_paused
        boolean is_monitored "default false"
        boolean is_present "false if DAG disappeared on last sync"
        json tags
        timestamp last_synced_at
        timestamp created_at
        timestamp updated_at
    }
    AUDIT_LOGS {
        uuid id PK
        uuid actor_user_id FK "nullable: system actions"
        string actor_type "USER | SYSTEM"
        string action
        string entity_type
        string entity_id
        json details "secrets never logged"
        string ip_address
        timestamp created_at
    }
```

**`ConnectionStatus` enum** (single source of truth, used by DB, API, and UI):
`UNKNOWN | HEALTHY | UNAUTHORIZED | FORBIDDEN | UNREACHABLE | TLS_ERROR | INVALID_ENDPOINT | AIRFLOW_ERROR`

**Audit log rules**: append-only (no update/delete endpoints or service methods). Audited actions in Plan 0:
`auth.login_success`, `auth.login_failed`, `auth.logout`, `user.create`, `user.update`, `user.deactivate`, `airflow_conn.create`, `airflow_conn.update`, `airflow_conn.delete`, `airflow_conn.test`, `airflow_dags.sync`, `monitored_dag.toggle`, `system.bootstrap_admin`.

---

### 3.2. Authentication & RBAC
- **Password hashing**: `pwdlib[argon2]` (Argon2id). Minimum password length 12.
- **Tokens** (`PyJWT`, HS256):
  - Access token: 15 min, claim `type="access"`, `sub`, `role`.
  - Refresh token: 7 days, claim `type="refresh"`, `jti` stored in `refresh_tokens`. **Rotated on every use**; reuse of a revoked token revokes the whole chain.
  - Decoders reject a token whose `type` does not match the expected use.
- **Token transport**:
  - Access token: returned in JSON, held **in memory** by the frontend (not localStorage).
  - Refresh token: `httpOnly`, `Secure` (outside development), `SameSite=Strict` cookie scoped to `/api/v1/auth`.
- **`get_current_user`**: decodes the access token, then **loads the user from the DB** and rejects if missing or `is_active=false`. Role checks use the DB role, so demotions take effect immediately.
- **Logout**: revokes the current refresh token and clears the cookie. Deactivating a user revokes all their refresh tokens.
- **Login rate limiting**: 5 failed attempts per email+IP per 15 min → `429`. In-memory store for Plan 0 (single process); swap for Redis later.
- **Roles**:

| Capability | Admin | Operator | Viewer |
|---|:-:|:-:|:-:|
| Manage users | ✅ | – | – |
| Create/update/delete Airflow connections | ✅ | – | – |
| Test connection, sync DAGs, toggle monitoring | ✅ | ✅ | – |
| View connections, DAGs, detailed health | ✅ | ✅ | ✅ |

- **Bootstrap**: on startup, if **no users exist**, create an Admin from `ADMIN_EMAIL` / `ADMIN_PASSWORD` and write a `system.bootstrap_admin` audit entry. Never overwrites existing users.

---

### 3.3. Airflow Orchestration Adapter

#### Adapter Interface (`orchestration/airflow/base.py`)
```python
class AirflowAdapter(Protocol):
    async def probe(self) -> ConnectionTestResult: ...      # status, latency_ms, api_version, airflow_version, message
    async def list_dags(self) -> list[AirflowDagSummary]: ...  # paginated internally
```
`factory.get_adapter(connection)` returns `MockAirflowAdapter` when `connection.kind == MOCK`, otherwise `LiveAirflowAdapter`.

#### Version Detection & Auth (Live Adapter)
`base_url` is stored as the host root (e.g. `http://localhost:8080`). On probe:
1. `GET {base_url}/api/v2/version` → success ⇒ Airflow 3.x, `api_version=v2`.
2. On 404, `GET {base_url}/api/v1/version` → success ⇒ Airflow 2.x, `api_version=v1`.
3. Detected versions are persisted on the connection; later calls use the stored prefix.

Authentication strategy per version:
| Airflow | `BASIC` auth_type | `TOKEN` auth_type |
|---|---|---|
| 2.x (`/api/v1`) | HTTP Basic header | `Authorization: Bearer <token>` |
| 3.x (`/api/v2`) | `POST /auth/token` with username/password → JWT, cached until expiry | `Authorization: Bearer <token>` |

Endpoint mapping: DAG list is `/api/v1/dags` or `/api/v2/dags`; schedule is read from `timetable_summary` (falling back to `schedule_interval` on older 2.x).

HTTP settings: `httpx.AsyncClient` with connect timeout 5s, read timeout 10s, `follow_redirects=False`, TLS verification on (per-connection `verify_tls` flag deferred).

#### Error Handling Matrix

| Scenario | Detected By | Status | User-Facing Message |
|---|---|---|---|
| Reachable & authenticated | `200` + JSON body | `HEALTHY` | Connection verified (Airflow {version}, {latency} ms). |
| Bad credentials | `401` | `UNAUTHORIZED` | Authentication failed. Check username and password/token. |
| Authenticated but lacks permission | `403` | `FORBIDDEN` | Credentials are valid but lack permission to read DAGs. Grant the user a Viewer role or higher in Airflow. |
| Host down / bad port / timeout / DNS | `ConnectError`, `TimeoutException` | `UNREACHABLE` | Cannot reach {base_url}. Ensure the Airflow API server is running and reachable from the backend. |
| Certificate problem | `SSL` errors | `TLS_ERROR` | TLS verification failed for {base_url}. Check the certificate. |
| Not an Airflow API | `404` on both `/api/v2` and `/api/v1` | `INVALID_ENDPOINT` | No Airflow REST API found at this URL. Use the webserver root, e.g. `http://host:8080`. |
| Login page / SSO proxy | `2xx`/`3xx` with non-JSON (HTML) body or redirect | `INVALID_ENDPOINT` | The URL returned a web page or redirect instead of the API. A proxy or SSO may be in front of Airflow. |
| Airflow internal error | `5xx` | `AIRFLOW_ERROR` | Airflow returned an internal error ({code}). Check the Airflow API server logs. |

#### Mock Adapter
- Selected **per connection** (`kind=MOCK`), so live and mock connections can coexist.
- Returns realistic DAGs (`daily_customer_etl`, `orders_pipeline`, `quality_checks`) and simulated latency. A mock base URL containing `?simulate=<status>` returns that status, for UI/error-path testing.
- **Blocked when `ENVIRONMENT=production`**: creating or probing a `MOCK` connection returns `400`.

#### Credential Handling
- Secrets encrypted with Fernet using `ENCRYPTION_KEY` (`core/crypto.py`); decrypted only inside the adapter factory.
- API responses never include the secret — only `has_secret: bool`.
- Audit `details` never contain secrets.
- Key rotation is supported via `MultiFernet` (`ENCRYPTION_KEY` may hold a comma-separated list; first key encrypts).

#### SSRF Mitigation (test-connection & create)
- Restricted to Admin/Operator.
- `base_url` must be `http`/`https`; redirects are not followed.
- Optional `AIRFLOW_ALLOWED_HOSTS` allowlist; when set, other hosts are rejected with `400`. Recommended in production.

---

### 3.4. Environment Configuration Reference (`.env.example`)

```env
# Application
PROJECT_NAME="Agentic Data Automation"
ENVIRONMENT="development"          # development | staging | production
DEBUG=True
API_V1_STR="/api/v1"
CORS_ORIGINS='["http://localhost:5173", "http://127.0.0.1:5173"]'

# Database
DATABASE_URL="sqlite:///./agentic_ai.db"
# For PostgreSQL: DATABASE_URL="postgresql+psycopg://postgres:postgres@localhost:5432/agentic_ai"
DB_POOL_SIZE=10
DB_MAX_OVERFLOW=20

# Security & JWT
JWT_SECRET_KEY="CHANGE_ME"         # generate: python -c "import secrets; print(secrets.token_urlsafe(64))"
JWT_ALGORITHM="HS256"
ACCESS_TOKEN_EXPIRE_MINUTES=15
REFRESH_TOKEN_EXPIRE_DAYS=7
LOGIN_MAX_ATTEMPTS=5
LOGIN_LOCKOUT_MINUTES=15

# Secret encryption (Fernet); comma-separated for rotation, first key encrypts
ENCRYPTION_KEY="CHANGE_ME"         # generate: python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"

# Initial Admin (used only when the users table is empty)
ADMIN_EMAIL="admin@agentic.local"
ADMIN_PASSWORD="CHANGE_ME"

# Airflow
AIRFLOW_ALLOWED_HOSTS=''           # e.g. '["airflow.internal", "localhost"]'; empty = allow all (dev only)
AIRFLOW_CONNECT_TIMEOUT_SECONDS=5
AIRFLOW_READ_TIMEOUT_SECONDS=10
```

**Startup safety checks** (`core/config.py`): when `ENVIRONMENT != development`, the app refuses to start if `JWT_SECRET_KEY`, `ENCRYPTION_KEY`, or `ADMIN_PASSWORD` equals `CHANGE_ME`, if `DEBUG=True`, or if `DATABASE_URL` is SQLite.

---

### 3.5. API Endpoints Specification

All errors use one shape: `{"error": {"code": "...", "message": "...", "details": {...}}}`. List endpoints use `?limit=&offset=` and return `{"items": [...], "total": n}`.

| Method | Endpoint | Description | Access |
|---|---|---|---|
| `GET` | `/api/v1/health/live` | Liveness only: `{"status":"ok"}` | Public |
| `GET` | `/api/v1/health` | DB status, per-connection Airflow status, app version | Viewer+ |
| `POST` | `/api/v1/auth/login` | Returns access token; sets refresh cookie | Public (rate-limited) |
| `POST` | `/api/v1/auth/refresh` | Rotates refresh cookie, returns new access token | Refresh cookie |
| `POST` | `/api/v1/auth/logout` | Revokes refresh token, clears cookie | Refresh cookie |
| `GET` | `/api/v1/auth/me` | Current user profile | Viewer+ |
| `GET` | `/api/v1/users` | List users (pagination, role filter) | Admin |
| `POST` | `/api/v1/users` | Create user with role | Admin |
| `PATCH` | `/api/v1/users/{id}` | Update name/role/active (cannot deactivate or demote the last active Admin) | Admin |
| `GET` | `/api/v1/airflow/connections` | List connections (no secrets) | Viewer+ |
| `POST` | `/api/v1/airflow/connections` | Create connection | Admin |
| `PATCH` | `/api/v1/airflow/connections/{id}` | Update connection (secret optional; omitted = unchanged) | Admin |
| `DELETE` | `/api/v1/airflow/connections/{id}` | Delete connection and its monitored DAGs | Admin |
| `POST` | `/api/v1/airflow/test-connection` | Test unsaved connection settings (body) | Operator+ |
| `POST` | `/api/v1/airflow/connections/{id}/test` | Test saved connection; updates `last_health_*` and detected versions | Operator+ |
| `POST` | `/api/v1/airflow/connections/{id}/sync-dags` | Fetch DAGs from Airflow, upsert `monitored_dags`, mark missing as `is_present=false` | Operator+ |
| `GET` | `/api/v1/airflow/connections/{id}/dags` | List stored DAGs (filter: `monitored`, `tag`, `search`) | Viewer+ |
| `PATCH` | `/api/v1/airflow/dags/{dag_pk}` | Set `is_monitored` | Operator+ |

---

### 3.6. Frontend Foundation & UI
- **Libraries**: `react-router` (routing). HTTP via a small `fetch` wrapper (no axios dependency needed). Types via JSDoc.
- **Dev proxy**: Vite proxies `/api` → `http://localhost:8000`, so the refresh cookie is same-origin in dev.
- **App shell**: Sidebar with **Dashboard** and **Settings** only. Header with global health pill (DB + Airflow, from `/api/v1/health`) and user menu (profile, logout).
- **Auth**:
  - Login page with validation and error display (including `429` lockout).
  - `AuthContext` keeps the access token in memory; on page load it calls `/auth/refresh` to restore the session.
  - API client attaches the Bearer token and, on `401`, performs **one** refresh (deduplicated across concurrent requests) then retries; on refresh failure it redirects to login.
  - Route guards by role (e.g. Users page is Admin-only; action buttons hidden for Viewer).
- **Settings → Airflow Connections**:
  - Form: name, environment, kind (Live/Mock; Mock hidden in production), base URL (host root), auth type, username, password/token.
  - "Test Connection" shows status, message, latency, detected Airflow version and API version.
  - Connection list with status pill and last-checked time.
- **Settings → Monitored DAGs**:
  - "Sync DAGs" button per connection; table of DAGs (id, schedule, paused, tags, present) with an `is_monitored` toggle.
- **Settings → Users** (Admin): list, create, change role, deactivate.

---

## 4. Implementation Steps & Milestones

Each phase is complete only when its listed tests pass.

### Phase 1: Backend Foundation (Config, DB, Migrations)
- [x] `requirements.txt`: `fastapi`, `uvicorn[standard]`, `pydantic-settings`, `email-validator`, `sqlalchemy>=2.0`, `alembic`, `psycopg[binary]`, `pwdlib[argon2]`, `PyJWT`, `cryptography`, `httpx`.
- [x] `requirements-dev.txt`: `pytest`, `pytest-asyncio`, `ruff`.
- [x] Move entry point to `app/main.py` (app factory); migrate existing `/health` to `/api/v1/health/live`.
- [x] `core/config.py` with startup safety checks; `.env.example`.
- [x] `db/session.py` (Postgres vs SQLite engine settings), `db/base.py` (naming convention, `TimestampMixin`).
- [x] Models: `User`, `RefreshToken`, `AirflowConnection`, `MonitoredDag`, `AuditLog` (with unique and partial-unique indexes).
- [x] Alembic with `render_as_batch=True`; generate initial migration.
- [x] `services/audit_service.py`, uniform error handler in `core/exceptions.py`.
- **Tests**: config rejects `CHANGE_ME` secrets outside development; migrations upgrade→downgrade→upgrade on SQLite and PostgreSQL; unique constraints enforced (email, connection name, one default connection, `(connection_id, dag_id)`).

### Phase 2: Authentication & Users
- [x] `core/security.py` (Argon2 hashing, typed JWT encode/decode), `core/crypto.py` (MultiFernet).
- [x] `auth_service` (login, refresh rotation with reuse detection, logout), `user_service`.
- [x] `dependencies.py`: `get_db`, DB-backed `get_current_user`, `require_role(*roles)`.
- [x] `core/rate_limit.py` for login.
- [x] `/api/v1/auth` and `/api/v1/users` endpoints, with audit entries.
- [x] `db/init_db.py` bootstrap admin (only when no users exist).
- **Tests**: login success/failure/lockout; refresh token cannot be used as access token and vice versa; refresh rotation and reuse revocation; deactivated user rejected immediately; role change takes effect on the next request; the last Admin cannot be demoted or deactivated; Viewer receives `403` on write endpoints; audit rows written for each audited action.

### Phase 3: Airflow Adapter & Monitored DAG Registry
- [x] `orchestration/airflow/base.py` (protocol, `ConnectionStatus`, DTOs).
- [x] `client.py`: version detection (v2 → v1), per-version auth (incl. Airflow 3 `/auth/token`), DAG pagination, error mapping per §3.3.
- [x] `mock.py` with `simulate` support; production guard.
- [x] `factory.py`; `airflow_service` (CRUD, test, sync, toggle); SSRF checks.
- [x] Airflow endpoints per §3.5; `/api/v1/health` detailed status.
- [x] `docker-compose.yml`: PostgreSQL 16 + Airflow 3.x (profile `airflow2` for 2.x).
- **Tests** (using `httpx.MockTransport`): every row of the error matrix, including HTML/redirect responses; v1 and v2 detection; Airflow 3 token acquisition and caching; secrets never appear in API responses or audit details; sync upserts, marks missing DAGs `is_present=false`, preserves `is_monitored`; mock connections rejected in production; `AIRFLOW_ALLOWED_HOSTS` enforced; Viewer cannot call test/sync.
- **Integration check** (manual/CI optional): test + sync against docker-compose Airflow 3.x and 2.x.

### Phase 4: Frontend Foundation
- [x] Add `react-router`; Vite `/api` proxy.
- [x] `index.css` design tokens (light/dark via `prefers-color-scheme`).
- [x] API client with in-memory token, single-flight refresh on `401`.
- [x] `AuthContext`, Login page, role-based route guards.
- [x] App shell (Sidebar: Dashboard, Settings; Header: health pill, user menu/logout).
- [x] Settings: Airflow Connections (create/edit/delete/test), Monitored DAGs (sync, toggle), Users (Admin).
- **Tests**: `oxlint` clean; manual/browser check of the flow in §5.4.

---

## 5. Definition of Done for Plan 0
1. **Auth & Security**: Phase 2 tests pass; no secrets in responses, logs, or audit details; app refuses to start in production with placeholder secrets.
2. **Database & Migrations**: migrations round-trip on SQLite and PostgreSQL; constraints enforced.
3. **Airflow**: error-matrix tests pass; test + DAG sync succeed against a real Airflow 3.x and 2.x (docker-compose) and a mock connection.
4. **End-to-end (browser)**: Admin logs in → creates an Operator → Operator logs in → registers an Airflow connection → tests it (sees version + latency) → syncs DAGs → marks 2 DAGs as monitored → Viewer logs in and sees them read-only → every step appears in `audit_logs`.

**Hand-off to Plan 1**: the set of `monitored_dags` with `is_monitored=true` is the input for failure/SLA detection (MVP step 2).
