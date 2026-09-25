from fastapi import APIRouter, Request

from app.core.config import get_settings
from app.core.dependencies import DbSession, OperatorUser, ViewerUser
from app.detection.scheduler import DetectionScheduler
from app.schemas.incident import CycleSummaryRead, DetectionStatusRead, LeaseRead
from app.services import detection_service
from app.services.detection_service import CycleSummary

router = APIRouter(prefix="/detection", tags=["detection"])


def _scheduler(request: Request) -> DetectionScheduler:
    return request.app.state.detector


def _summary(summary: CycleSummary) -> CycleSummaryRead:
    return CycleSummaryRead.model_validate(summary, from_attributes=True)


@router.post("/run", response_model=CycleSummaryRead)
def run_detection(request: Request, operator: OperatorUser) -> CycleSummaryRead:
    summary = _scheduler(request).run_once(trigger="manual", actor_id=operator.id)
    return _summary(summary)  # type: ignore[arg-type]


@router.get("/status", response_model=DetectionStatusRead)
def detection_status(request: Request, db: DbSession, _: ViewerUser) -> DetectionStatusRead:
    scheduler = _scheduler(request)
    lease = detection_service.get_lease(db)
    last = scheduler.last_summary
    return DetectionStatusRead(
        enabled=get_settings().DETECTION_ENABLED,
        loop_running=scheduler.running_loop,
        interval_seconds=scheduler.interval_seconds,
        this_worker=scheduler.holder,
        lease=LeaseRead(holder=lease.holder, expires_at=lease.expires_at) if lease else None,
        last_cycle=_summary(last) if isinstance(last, CycleSummary) else None,
        last_error=scheduler.last_error,
    )
