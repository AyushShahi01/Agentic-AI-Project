"""In-process detection loop guarded by a DB lease (one active detector across workers)."""

import logging
import os
import socket
import threading
import uuid
from collections.abc import Callable
from datetime import datetime

import anyio
import anyio.to_thread
from sqlalchemy.orm import Session, sessionmaker

from app.core.exceptions import ConflictError
from app.db.base import utcnow
from app.models.user import User

logger = logging.getLogger(__name__)

STARTUP_DELAY_SECONDS = 5


class DetectionBusyError(ConflictError):
    code = "detection_busy"

    def __init__(self) -> None:
        super().__init__("A detection cycle is already running. Try again shortly.")


class DetectionScheduler:
    """Owns the polling loop and serializes cycles within this process.

    `run_cycle` is injected (normally `detection_service.run_cycle`) to keep this module free of
    service-layer imports.
    """

    def __init__(
        self,
        session_factory: sessionmaker[Session],
        *,
        interval_seconds: int,
        run_cycle: Callable[..., object],
        acquire_lease: Callable[[Session, str, int, datetime], bool],
    ) -> None:
        self.session_factory = session_factory
        self.interval_seconds = interval_seconds
        self.holder = f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:8]}"
        self.last_summary: object | None = None
        self.last_error: str | None = None
        self.running_loop = False
        self._run_cycle = run_cycle
        self._acquire_lease = acquire_lease
        self._lock = threading.Lock()

    @property
    def lease_ttl_seconds(self) -> int:
        return max(self.interval_seconds * 2, 60)

    def run_once(self, *, trigger: str, actor_id: uuid.UUID | None = None) -> object:
        """Run one cycle now. Raises DetectionBusyError if another cycle holds the lock/lease."""
        if not self._lock.acquire(blocking=False):
            raise DetectionBusyError()
        try:
            with self.session_factory() as db:
                if not self._acquire_lease(db, self.holder, self.lease_ttl_seconds, utcnow()):
                    raise DetectionBusyError()
                actor = db.get(User, actor_id) if actor_id else None
                summary = self._run_cycle(db, trigger=trigger, actor=actor)
                self.last_summary = summary
                self.last_error = None
                return summary
        finally:
            self._lock.release()

    def _scheduled_cycle(self) -> None:
        try:
            self.run_once(trigger="schedule")
        except DetectionBusyError:
            logger.debug("Detection lease held elsewhere; skipping this cycle")
        except Exception as exc:
            self.last_error = str(exc)[:500]
            logger.exception("Scheduled detection cycle failed")

    async def run_forever(self) -> None:
        self.running_loop = True
        try:
            await anyio.sleep(STARTUP_DELAY_SECONDS)
            while True:
                await anyio.to_thread.run_sync(self._scheduled_cycle)
                await anyio.sleep(self.interval_seconds)
        finally:
            self.running_loop = False
