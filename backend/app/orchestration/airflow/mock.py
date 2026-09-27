"""Mock Airflow adapter for offline development and UI testing.

Append `?simulate=<STATUS>` to a mock connection's base URL to force a failure status,
e.g. `http://mock-airflow?simulate=UNAUTHORIZED`.

Run history is derived from the clock, so repeated polls are consistent:
- `orders_pipeline`       every 5 min; every 3rd run fails in `load_orders`, the next succeeds.
- `quality_checks`        every 30 min, always succeeds.
- `daily_customer_etl`    daily at 02:00 UTC, always succeeds.
- `legacy_inventory_sync` paused; last success 3 days ago (breaches any SLA).
- `partner_api_sync`      every 10 min; every 4th run fails in `fetch_partner_orders` with a
                          transient network error (HTTP 503), the next succeeds.

Write calls (Plan 2) keep in-memory state per base URL, driven by the same clock:
- clearing a run makes it `running` for RERUN_SECONDS, then it ends: transient failures succeed,
  `orders_pipeline` (a data error) fails again;
- triggered runs are `running` for RERUN_SECONDS, then succeed;
- pause/unpause changes `is_paused` in `list_dags`/`get_dag`.
"""

import asyncio
import random
from collections.abc import Callable
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.parse import parse_qs, urlsplit

from app.orchestration.airflow.base import (
    AdapterConfig,
    AirflowAdapterError,
    AirflowDagRun,
    AirflowDagSummary,
    AirflowTaskInstance,
    ApiVersion,
    ConnectionStatus,
    ConnectionTestResult,
    TaskLog,
    describe_status,
    tail_text,
)

MOCK_AIRFLOW_VERSION = "3.1.0-mock"

MOCK_DAGS: tuple[AirflowDagSummary, ...] = (
    AirflowDagSummary(
        dag_id="daily_customer_etl",
        description="Extracts customers from CRM, loads to warehouse `dim_customer`.",
        schedule_summary="0 2 * * *",
        is_paused=False,
        tags=["etl", "customers", "tier-1"],
    ),
    AirflowDagSummary(
        dag_id="orders_pipeline",
        description="Ingestion of order events into `fact_orders` every 5 minutes.",
        schedule_summary="*/5 * * * *",
        is_paused=False,
        tags=["ingestion", "orders", "tier-1"],
    ),
    AirflowDagSummary(
        dag_id="quality_checks",
        description="Freshness, null-rate and row-count checks for core tables.",
        schedule_summary="*/30 * * * *",
        is_paused=False,
        tags=["data-quality"],
    ),
    AirflowDagSummary(
        dag_id="legacy_inventory_sync",
        description="Deprecated nightly inventory sync.",
        schedule_summary="0 4 * * *",
        is_paused=True,
        tags=["inventory", "legacy"],
    ),
    AirflowDagSummary(
        dag_id="partner_api_sync",
        description="Pulls partner orders from the partner REST API every 10 minutes.",
        schedule_summary="*/10 * * * *",
        is_paused=False,
        tags=["ingestion", "partner"],
    ),
)

# Test hook: replace to control the simulated time.
clock: Callable[[], datetime] = lambda: datetime.now(UTC)  # noqa: E731

_DEFAULT_LOOKBACK = timedelta(hours=24)
_SCHEDULES: dict[str, timedelta] = {
    "orders_pipeline": timedelta(minutes=5),
    "quality_checks": timedelta(minutes=30),
    "daily_customer_etl": timedelta(days=1),
    "partner_api_sync": timedelta(minutes=10),
}
_SCHEDULE_OFFSETS: dict[str, timedelta] = {"daily_customer_etl": timedelta(hours=2)}
_TASKS: dict[str, list[str]] = {
    "orders_pipeline": ["extract_orders", "transform_orders", "load_orders", "notify_downstream"],
    "quality_checks": ["check_freshness", "check_null_rates", "check_row_counts"],
    "daily_customer_etl": ["extract_customers", "load_dim_customer"],
    "legacy_inventory_sync": ["sync_inventory"],
    "partner_api_sync": ["fetch_partner_orders", "load_partner_orders"],
}
# dag_id -> (failing task, every Nth run fails)
_FAILURES: dict[str, tuple[str, int]] = {
    "orders_pipeline": ("load_orders", 3),
    "partner_api_sync": ("fetch_partner_orders", 4),
}
# Failures that are not transient: clearing the run fails again.
_PERSISTENT_FAILURES = frozenset({"orders_pipeline"})
RERUN_SECONDS = 60

_FAILURE_LOG = """\
[{start}] INFO - Dependencies all met for <TaskInstance: orders_pipeline.load_orders {run_id}>
[{start}] INFO - Starting attempt 2 of 2
[{start}] INFO - Executing <Task(PythonOperator): load_orders> on {logical}
[{start}] INFO - Connecting to postgres://etl_user:hunter2@warehouse.internal:5432/dw
[{start}] INFO - Loading 18,442 rows into fact_orders
[{end}] ERROR - Task failed with exception
Traceback (most recent call last):
  File "/opt/airflow/dags/orders_pipeline.py", line 88, in load_orders
    cursor.executemany(INSERT_SQL, batch)
  File "/usr/local/lib/python3.12/site-packages/psycopg/cursor.py", line 118, in executemany
    raise ex.with_traceback(None)
psycopg.errors.UniqueViolation: duplicate key value violates unique constraint "fact_orders_pkey"
DETAIL:  Key (order_id)=(A-10492) already exists.
[{end}] INFO - Marking task as FAILED. dag_id=orders_pipeline, task_id=load_orders
"""

_TRANSIENT_LOG = """\
[{start}] INFO - Dependencies all met for <TaskInstance: partner_api_sync.fetch_partner_orders \
{run_id}>
[{start}] INFO - Starting attempt 3 of 3
[{start}] INFO - GET https://partner-api.example.com/v2/orders?since={logical}
[{end}] WARNING - Retrying (Retry(total=0)) after connection broken
[{end}] ERROR - Task failed with exception
Traceback (most recent call last):
  File "/opt/airflow/dags/partner_api_sync.py", line 41, in fetch_partner_orders
    response.raise_for_status()
requests.exceptions.HTTPError: 503 Server Error: Service Unavailable for url: \
https://partner-api.example.com/v2/orders
[{end}] INFO - Marking task as FAILED. dag_id=partner_api_sync, task_id=fetch_partner_orders
"""

_FAILURE_LOGS = {"orders_pipeline": _FAILURE_LOG, "partner_api_sync": _TRANSIENT_LOG}


@dataclass
class _WriteState:
    """Remediation calls made against one mock connection."""

    cleared: dict[tuple[str, str], datetime] = field(default_factory=dict)  # (dag, run) -> at
    triggered: dict[str, list[datetime]] = field(default_factory=dict)  # dag -> trigger times
    paused: dict[str, bool] = field(default_factory=dict)


_state: dict[str, _WriteState] = {}


def reset_state() -> None:
    """Forget all simulated write calls (tests)."""
    _state.clear()


def _run(dag_id: str, logical: datetime, state: str, duration: timedelta | None) -> AirflowDagRun:
    start = logical + timedelta(seconds=5)
    return AirflowDagRun(
        run_id=f"scheduled__{logical.isoformat()}",
        state=state,
        logical_date=logical,
        start_date=start,
        end_date=start + duration if duration else None,
        run_type="scheduled",
    )


def _runs_for(dag_id: str, now: datetime, since: datetime) -> list[AirflowDagRun]:
    """Newest first, logical_date >= since."""
    if dag_id == "legacy_inventory_sync":
        logical = (now - timedelta(days=3)).replace(minute=0, second=0, microsecond=0)
        run = _run(dag_id, logical, "success", timedelta(minutes=4))
        return [run] if logical >= since else []

    period = _SCHEDULES.get(dag_id)
    if period is None:
        return []
    seconds = int(period.total_seconds())
    offset = int(_SCHEDULE_OFFSETS.get(dag_id, timedelta()).total_seconds())
    current = (int(now.timestamp()) - offset) // seconds
    runs: list[AirflowDagRun] = []
    bucket = current
    while True:
        logical = datetime.fromtimestamp(bucket * seconds + offset, UTC)
        if logical < since:
            break
        if bucket == current and now - logical < timedelta(minutes=2):
            runs.append(_run(dag_id, logical, "running", None))
        elif dag_id in _FAILURES and bucket % _FAILURES[dag_id][1] == 0:
            runs.append(_run(dag_id, logical, "failed", timedelta(seconds=75)))
        else:
            runs.append(_run(dag_id, logical, "success", timedelta(seconds=95)))
        bucket -= 1
    return runs


def _apply_writes(
    dag_id: str, runs: list[AirflowDagRun], state: _WriteState, now: datetime, since: datetime
) -> list[AirflowDagRun]:
    """Overlay cleared and triggered runs on the clock-derived history. Newest first."""
    rerun = timedelta(seconds=RERUN_SECONDS)
    result = []
    for run in runs:
        cleared_at = state.cleared.get((dag_id, run.run_id))
        if cleared_at is not None and cleared_at <= now:
            if now - cleared_at < rerun:
                run = replace(run, state="running", start_date=cleared_at, end_date=None)
            else:
                outcome = "failed" if dag_id in _PERSISTENT_FAILURES else "success"
                run = replace(
                    run, state=outcome, start_date=cleared_at, end_date=cleared_at + rerun
                )
        result.append(run)
    for when in state.triggered.get(dag_id, []):
        if when < since or when > now:
            continue
        done = now - when >= rerun
        result.append(
            AirflowDagRun(
                run_id=f"manual__{when.isoformat()}",
                state="success" if done else "running",
                logical_date=when,
                start_date=when,
                end_date=when + rerun if done else None,
                run_type="manual",
            )
        )
    return sorted(result, key=lambda r: r.sort_date or now, reverse=True)


def _task_instances(dag_id: str, run: AirflowDagRun) -> list[AirflowTaskInstance]:
    tasks = _TASKS.get(dag_id, [])
    failing_task = _FAILURES.get(dag_id, ("", 0))[0]
    cursor = run.start_date or run.logical_date
    upstream_failed = False
    instances = []
    for task_id in tasks:
        if run.state == "failed" and task_id == failing_task:
            state, tries = "failed", 2
        elif upstream_failed:
            state, tries = "upstream_failed", 1
        elif run.state == "running" and task_id == tasks[-1]:
            state, tries = "running", 1
        else:
            state, tries = "success", 1
        started = cursor if state != "upstream_failed" else None
        ended = (
            cursor + timedelta(seconds=20) if state in ("success", "failed") and cursor else None
        )
        instances.append(
            AirflowTaskInstance(
                task_id=task_id,
                state=state,
                try_number=tries,
                start_date=started,
                end_date=ended,
                operator="PythonOperator",
            )
        )
        upstream_failed = upstream_failed or state == "failed"
        cursor = ended or cursor
    return instances


class MockAirflowAdapter:
    def __init__(self, config: AdapterConfig, *, simulate_latency: bool = True) -> None:
        self.config = config
        self._simulate_latency = simulate_latency
        self.simulated_status = _parse_simulated_status(config.base_url)

    async def _latency(self) -> int:
        latency_ms = random.randint(20, 60)
        if self._simulate_latency:
            await asyncio.sleep(latency_ms / 1000)
        return latency_ms

    def _failure(self, latency_ms: int | None = None) -> ConnectionTestResult:
        return ConnectionTestResult(
            status=self.simulated_status,
            message=describe_status(self.simulated_status, self.config.base_url, http_status=500),
            latency_ms=latency_ms,
        )

    def _raise_if_simulating(self) -> None:
        if self.simulated_status != ConnectionStatus.HEALTHY:
            raise AirflowAdapterError(self._failure())

    @property
    def _writes(self) -> _WriteState:
        return _state.setdefault(self.config.base_url, _WriteState())

    def _dag(self, dag: AirflowDagSummary) -> AirflowDagSummary:
        paused = self._writes.paused.get(dag.dag_id)
        return dag if paused is None else replace(dag, is_paused=paused)

    def _runs(self, dag_id: str, since: datetime) -> list[AirflowDagRun]:
        now = clock()
        return _apply_writes(dag_id, _runs_for(dag_id, now, since), self._writes, now, since)

    def _find_run(self, dag_id: str, run_id: str) -> AirflowDagRun | None:
        runs = self._runs(dag_id, clock() - timedelta(days=4))
        return next((r for r in runs if r.run_id == run_id), None)

    async def probe(self) -> ConnectionTestResult:
        latency_ms = await self._latency()
        if self.simulated_status != ConnectionStatus.HEALTHY:
            return self._failure(latency_ms)
        return ConnectionTestResult(
            status=ConnectionStatus.HEALTHY,
            message=describe_status(
                ConnectionStatus.HEALTHY,
                self.config.base_url,
                airflow_version=MOCK_AIRFLOW_VERSION,
                latency_ms=latency_ms,
            ),
            latency_ms=latency_ms,
            api_version=ApiVersion.V2,
            airflow_version=MOCK_AIRFLOW_VERSION,
        )

    async def list_dags(self) -> list[AirflowDagSummary]:
        await self._latency()
        self._raise_if_simulating()
        return [self._dag(d) for d in MOCK_DAGS]

    async def list_dag_runs(
        self, dag_id: str, *, since: datetime | None = None, limit: int = 100
    ) -> list[AirflowDagRun]:
        self._raise_if_simulating()
        return self._runs(dag_id, since or clock() - _DEFAULT_LOOKBACK)[:limit]

    async def list_task_instances(self, dag_id: str, run_id: str) -> list[AirflowTaskInstance]:
        self._raise_if_simulating()
        run = self._find_run(dag_id, run_id)
        return _task_instances(dag_id, run) if run else []

    async def get_task_log(
        self, dag_id: str, run_id: str, task_id: str, try_number: int, *, max_bytes: int
    ) -> TaskLog:
        self._raise_if_simulating()
        run = self._find_run(dag_id, run_id)
        if run is None:
            return TaskLog(content="", available=False)
        start = run.start_date or clock()
        if run.state == "failed" and task_id == _FAILURES.get(dag_id, ("", 0))[0]:
            text = _FAILURE_LOGS[dag_id].format(
                start=start.isoformat(),
                end=(run.end_date or start).isoformat(),
                run_id=run_id,
                logical=run.logical_date.isoformat() if run.logical_date else "",
            )
        else:
            text = f"[{start.isoformat()}] INFO - Task {task_id} exited with return code 0\n"
        content, truncated = tail_text(text, max_bytes)
        return TaskLog(content=content, truncated=truncated)

    # ------------------------------------------------------------------ write path

    async def get_dag(self, dag_id: str) -> AirflowDagSummary | None:
        self._raise_if_simulating()
        return next((self._dag(d) for d in MOCK_DAGS if d.dag_id == dag_id), None)

    async def get_dag_run(self, dag_id: str, run_id: str) -> AirflowDagRun | None:
        self._raise_if_simulating()
        return self._find_run(dag_id, run_id)

    async def clear_task_instances(
        self, dag_id: str, run_id: str, *, only_failed: bool = True, include_downstream: bool = True
    ) -> list[str]:
        self._raise_if_simulating()
        run = self._find_run(dag_id, run_id)
        if run is None:
            raise AirflowAdapterError(
                ConnectionTestResult(
                    status=ConnectionStatus.AIRFLOW_ERROR,
                    message=f"Airflow rejected the request (404): run {run_id} not found",
                )
            )
        instances = _task_instances(dag_id, run)
        failing = _FAILURES.get(dag_id, ("", 0))[0]
        cleared = [
            ti.task_id
            for ti in instances
            if not only_failed
            or ti.state == "failed"
            or (include_downstream and ti.state == "upstream_failed")
        ]
        if cleared and not include_downstream:
            cleared = [t for t in cleared if t == failing] or cleared[:1]
        if cleared:
            self._writes.cleared[(dag_id, run_id)] = clock()
        return cleared

    async def trigger_dag_run(
        self, dag_id: str, *, note: str | None = None, conf: dict[str, Any] | None = None
    ) -> AirflowDagRun:
        self._raise_if_simulating()
        when = clock()
        self._writes.triggered.setdefault(dag_id, []).append(when)
        return AirflowDagRun(
            run_id=f"manual__{when.isoformat()}",
            state="queued",
            logical_date=when,
            run_type="manual",
        )

    async def set_dag_paused(self, dag_id: str, paused: bool) -> None:
        self._raise_if_simulating()
        self._writes.paused[dag_id] = paused


def _parse_simulated_status(base_url: str) -> ConnectionStatus:
    values = parse_qs(urlsplit(base_url).query).get("simulate")
    if not values:
        return ConnectionStatus.HEALTHY
    try:
        status = ConnectionStatus(values[0].upper())
    except ValueError:
        return ConnectionStatus.HEALTHY
    return ConnectionStatus.HEALTHY if status == ConnectionStatus.UNKNOWN else status
