from collections.abc import Awaitable, Callable

import anyio
import anyio.from_thread
import httpx

from app.orchestration.airflow.base import AdapterConfig, AirflowAdapter
from app.orchestration.airflow.client import LiveAirflowAdapter
from app.orchestration.airflow.mock import MockAirflowAdapter

# Test hook: inject an httpx transport (e.g. httpx.MockTransport) into live adapters.
live_transport: httpx.AsyncBaseTransport | None = None


def build_adapter(*, mock: bool, config: AdapterConfig) -> AirflowAdapter:
    if mock:
        return MockAirflowAdapter(config)
    return LiveAirflowAdapter(config, transport=live_transport)


def run_async[T](fn: Callable[[], Awaitable[T]]) -> T:
    """Run an adapter coroutine from sync code.

    Inside a FastAPI sync endpoint (an AnyIO worker thread) the coroutine runs on the app's
    event loop; elsewhere (CLI, scripts) a new event loop is started.
    """
    try:
        return anyio.from_thread.run(fn)
    except anyio.NoEventLoopError:
        return anyio.run(fn)
