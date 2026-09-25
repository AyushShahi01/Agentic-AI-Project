import threading
import time
from collections import deque
from collections.abc import Callable

from app.core.exceptions import RateLimitedError


class LoginRateLimiter:
    """In-memory sliding-window limiter for failed logins (single process only).

    Replace with a shared store (e.g. Redis) before running multiple workers.
    """

    def __init__(
        self,
        max_attempts: int,
        window_seconds: float,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.max_attempts = max_attempts
        self.window_seconds = window_seconds
        self._clock = clock
        self._failures: dict[str, deque[float]] = {}
        self._lock = threading.Lock()

    @staticmethod
    def key(email: str, ip: str | None) -> str:
        return f"{email.strip().lower()}|{ip or '-'}"

    def _prune(self, key: str, now: float) -> deque[float]:
        attempts = self._failures.setdefault(key, deque())
        while attempts and now - attempts[0] >= self.window_seconds:
            attempts.popleft()
        return attempts

    def check(self, key: str) -> None:
        with self._lock:
            now = self._clock()
            attempts = self._prune(key, now)
            if len(attempts) >= self.max_attempts:
                retry_after = int(self.window_seconds - (now - attempts[0])) + 1
                raise RateLimitedError(
                    "Too many failed login attempts. Try again later.",
                    details={"retry_after_seconds": retry_after},
                    headers={"Retry-After": str(retry_after)},
                )

    def record_failure(self, key: str) -> None:
        with self._lock:
            self._prune(key, self._clock()).append(self._clock())

    def reset(self, key: str) -> None:
        with self._lock:
            self._failures.pop(key, None)
