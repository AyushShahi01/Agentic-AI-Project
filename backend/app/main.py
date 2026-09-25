import asyncio
import contextlib
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

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
from app.services import automation_service, detection_service
from app.services.detection_service import CycleSummary

logger = logging.getLogger(__name__)


def run_cycle_with_automation(
    db: Session, *, trigger: str = "schedule", actor: User | None = None
) -> CycleSummary:
    """One scheduler cycle: detection, then advance automation runs (same lease)."""
    summary = detection_service.run_cycle(db, trigger=trigger, actor=actor)
    if get_settings().AUTOMATION_ENABLED:
        try:
            summary.automation = automation_service.tick(db).as_dict()
        except Exception as exc:
            db.rollback()
            logger.exception("Automation tick failed")
            summary.automation = {"error": str(exc)[:300]}
    return summary


def create_app(
    *,
    session_factory: sessionmaker[Session] | None = None,
    bootstrap: bool = True,
    detection: bool | None = None,
) -> FastAPI:
    """`detection=None` follows DETECTION_ENABLED; tests pass False to skip the polling loop."""
    settings = get_settings()
    logging.basicConfig(level=logging.DEBUG if settings.DEBUG else logging.INFO)
    factory = session_factory or SessionLocal
    start_detection = settings.DETECTION_ENABLED if detection is None else detection
    detector = DetectionScheduler(
        factory,
        interval_seconds=settings.DETECTION_INTERVAL_SECONDS,
        run_cycle=run_cycle_with_automation,
        acquire_lease=detection_service.try_acquire_lease,
    )

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        if bootstrap:
            await run_in_threadpool(run_bootstrap, factory)
        task = asyncio.create_task(detector.run_forever()) if start_detection else None
        yield
        if task is not None:
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
