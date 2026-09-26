import asyncio
import contextlib
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from functools import partial

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session, sessionmaker
from starlette.concurrency import run_in_threadpool

from app.api.v1.router import api_router
from app.core.config import get_settings
from app.core.exceptions import register_exception_handlers
from app.core.rate_limit import LoginRateLimiter
from app.db.init_db import run_bootstrap
from app.db.session import SessionLocal
from app.detection.scheduler import DetectionScheduler
from app.models.user import User
from app.services import automation_service, connection_monitor_service, detection_service
from app.services.detection_service import CycleSummary

logger = logging.getLogger(__name__)


def run_detection_cycle(
    db: Session, *, trigger: str = "schedule", actor: User | None = None
) -> CycleSummary:
    """One detection cycle. A manual run ("Run detection now") also advances automation right
    away so the operator sees the result; scheduled runs leave that to the automation loop."""
    summary = detection_service.run_cycle(db, trigger=trigger, actor=actor)
    if trigger == "manual" and get_settings().AUTOMATION_ENABLED:
        try:
            summary.automation = automation_service.tick(db).as_dict()
        except Exception as exc:
            db.rollback()
            logger.exception("Automation tick failed")
            summary.automation = {"error": str(exc)[:300]}
    return summary


def run_automation_tick(
    db: Session, *, trigger: str = "schedule", actor: User | None = None
) -> automation_service.TickSummary:
    return automation_service.tick(db)


def create_app(
    *,
    session_factory: sessionmaker[Session] | None = None,
    bootstrap: bool = True,
    detection: bool | None = None,
) -> FastAPI:
    """`detection=None` follows DETECTION_ENABLED (and AUTOMATION_ENABLED for the automation
    loop); tests pass False to skip both polling loops."""
    settings = get_settings()
    logging.basicConfig(level=logging.DEBUG if settings.DEBUG else logging.INFO)
    factory = session_factory or SessionLocal
    start_detection = settings.DETECTION_ENABLED if detection is None else detection
    detector = DetectionScheduler(
        factory,
        interval_seconds=settings.DETECTION_INTERVAL_SECONDS,
        run_cycle=run_detection_cycle,
        acquire_lease=detection_service.try_acquire_lease,
        name="detection",
    )
    automation_loop = DetectionScheduler(
        factory,
        interval_seconds=settings.AUTOMATION_INTERVAL_SECONDS,
        run_cycle=run_automation_tick,
        acquire_lease=partial(detection_service.try_acquire_lease, name="automation"),
        name="automation",
    )
    start_automation = settings.AUTOMATION_ENABLED if detection is None else detection
    connection_monitor = DetectionScheduler(
        factory,
        interval_seconds=settings.AIRFLOW_MONITOR_INTERVAL_SECONDS,
        run_cycle=connection_monitor_service.run_cycle,
        acquire_lease=partial(
            detection_service.try_acquire_lease, name=connection_monitor_service.LEASE_NAME
        ),
        name="connections",
    )
    start_monitor = settings.AIRFLOW_MONITOR_ENABLED if detection is None else detection

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        if bootstrap:
            await run_in_threadpool(run_bootstrap, factory)
        tasks = [
            asyncio.create_task(loop.run_forever())
            for loop, enabled in (
                (detector, start_detection),
                (automation_loop, start_automation),
                (connection_monitor, start_monitor),
            )
            if enabled
        ]
        yield
        for task in tasks:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

    app = FastAPI(
        title=settings.PROJECT_NAME,
        version=settings.VERSION,
        debug=settings.DEBUG,
        lifespan=lifespan,
        docs_url="/docs" if settings.is_development else None,
        redoc_url=None,
    )
    app.state.session_factory = factory
    app.state.detector = detector
    app.state.automation_loop = automation_loop
    app.state.connection_monitor = connection_monitor
    app.state.login_limiter = LoginRateLimiter(
        settings.LOGIN_MAX_ATTEMPTS, settings.LOGIN_LOCKOUT_MINUTES * 60
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.CORS_ORIGINS,
        allow_credentials=True,
        allow_methods=["GET", "POST", "PATCH", "DELETE", "OPTIONS"],
        allow_headers=["Authorization", "Content-Type"],
    )
    register_exception_handlers(app)
    app.include_router(api_router, prefix=settings.API_V1_STR)
    return app


app = create_app()
