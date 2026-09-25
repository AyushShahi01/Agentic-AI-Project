"""Backend for browser (Playwright) tests: a fresh SQLite database, seeded data, and the mock
Airflow clock pinned to just after a `partner_api_sync` failure (then running in real time).

    python scripts/e2e_server.py [--port 8000] [--db PATH]

Seeds: admin (E2E_ADMIN_EMAIL / E2E_ADMIN_PASSWORD), the built-in workflows with
`retry-transient` turned on, and a DEV mock connection whose `partner_api_sync` and
`orders_pipeline` DAGs are monitored. Detection does not poll; tests use "Run detection now".
Never use this for anything but local tests.
"""

import argparse
import os
import sys
import tempfile
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

BACKEND = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND))

parser = argparse.ArgumentParser()
parser.add_argument("--port", type=int, default=8000)
parser.add_argument("--db", default=str(Path(tempfile.gettempdir()) / "agentic_e2e.db"))
args = parser.parse_args()

db_path = Path(args.db)
db_path.unlink(missing_ok=True)
os.environ.update(
    {
        "ENVIRONMENT": "development",
        "DATABASE_URL": f"sqlite:///{db_path.as_posix()}",
        "ADMIN_EMAIL": os.getenv("E2E_ADMIN_EMAIL", "admin@example.com"),
        "ADMIN_PASSWORD": os.getenv("E2E_ADMIN_PASSWORD", "e2e-admin-password-123"),
        "DETECTION_ENABLED": "false",
        "AIRFLOW_ALLOWED_HOSTS": "[]",
    }
)

from sqlalchemy import select  # noqa: E402

from app.db.base import Base  # noqa: E402
from app.db.init_db import run_bootstrap  # noqa: E402
from app.db.session import SessionLocal, engine  # noqa: E402
from app.models.airflow import ConnectionKind, DeploymentEnvironment  # noqa: E402
from app.models.automation import Workflow  # noqa: E402
from app.models.user import User  # noqa: E402
from app.orchestration.airflow import mock  # noqa: E402
from app.schemas.airflow import AirflowConnCreate  # noqa: E402
from app.services import airflow_service  # noqa: E402

# Pin the mock clock: 30s into a partner_api_sync bucket whose previous run failed.
current = int(time.time()) // 600
bucket = current - ((current - 1) % 4)
base = datetime.fromtimestamp(bucket * 600 + 30, UTC)
started = time.time()
mock.clock = lambda: base + timedelta(seconds=time.time() - started)

Base.metadata.create_all(engine)
run_bootstrap(SessionLocal)
with SessionLocal() as db:
    admin = db.scalars(select(User)).one()
    conn = airflow_service.create_connection(
        db,
        AirflowConnCreate(
            name="mock-dev",
            environment=DeploymentEnvironment.DEV,
            kind=ConnectionKind.MOCK,
            base_url="http://mock-airflow",
            auth_type="NONE",
        ),
        actor=admin,
        ip_address=None,
    )
    airflow_service.sync_dags(db, conn.id, actor=admin, ip_address=None)
    for dag in conn.dags:
        if dag.dag_id in ("partner_api_sync", "orders_pipeline"):
            dag.is_monitored = True
    db.scalar(select(Workflow).where(Workflow.key == "retry-transient")).enabled = True
    db.commit()

import uvicorn  # noqa: E402

uvicorn.run("app.main:app", host="127.0.0.1", port=args.port, log_level="warning")
