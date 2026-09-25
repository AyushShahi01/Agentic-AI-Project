"""Plan 1 / Phase 1: adapter read path (runs, task instances, logs) for Airflow 2 and 3 + mock."""

from datetime import UTC, datetime, timedelta

import httpx
import pytest

from app.orchestration.airflow import mock as mock_module
from app.orchestration.airflow.base import AdapterConfig, AuthType, tail_text
from app.orchestration.airflow.client import LiveAirflowAdapter
from app.orchestration.airflow.mock import MockAirflowAdapter
from tests.fake_airflow import FakeAirflow

BASE = "http://airflow.test:8080"


def live(fake: FakeAirflow) -> LiveAirflowAdapter:
    config = AdapterConfig(
        base_url=BASE, auth_type=AuthType.BASIC, username="admin", secret="admin"
    )
    return LiveAirflowAdapter(config, transport=fake.transport())


def seeded(major: int) -> FakeAirflow:
    fake = FakeAirflow(major=major)
    fake.add_run("etl", "r1", "success", "2026-09-24T10:00:00+00:00", "2026-09-24T10:05:00+00:00")
    fake.add_run(
        "etl",
        "r2",
        "failed",
        "2026-09-24T11:00:00+00:00",
        "2026-09-24T11:02:00+00:00",
        failed_task="load",
        log="line one\nboom: ValueError\nlast line",
    )
    return fake


@pytest.mark.parametrize("major", [2, 3])
async def test_list_dag_runs_parses_and_orders_newest_first(major: int) -> None:
    runs = await live(seeded(major)).list_dag_runs("etl")
    assert [r.run_id for r in runs] == ["r2", "r1"]
    assert runs[0].state == "failed"
    assert runs[0].logical_date == datetime(2026, 9, 24, 11, tzinfo=UTC)
    assert runs[0].end_date == datetime(2026, 9, 24, 11, 2, tzinfo=UTC)


@pytest.mark.parametrize(("major", "param"), [(2, "execution_date_gte"), (3, "logical_date_gte")])
async def test_since_filter_uses_version_specific_param(major: int, param: str) -> None:
    fake = seeded(major)
    runs = await live(fake).list_dag_runs("etl", since=datetime(2026, 9, 24, 10, 30, tzinfo=UTC))
    assert [r.run_id for r in runs] == ["r2"]
    assert fake.last_run_params[param] == "2026-09-24T10:30:00Z"


@pytest.mark.parametrize("major", [2, 3])
async def test_task_instances_and_log(major: int) -> None:
    adapter = live(seeded(major))
    tis = await adapter.list_task_instances("etl", "r2")
    failed = next(t for t in tis if t.state == "failed")
    assert (failed.task_id, failed.try_number) == ("load", 2)
    log = await adapter.get_task_log("etl", "r2", "load", 2, max_bytes=10_000)
    assert log.available and "boom: ValueError" in log.content
    if major == 3:  # structured events flattened with a prefix
        assert "[t info] boom: ValueError" in log.content


async def test_airflow2_repr_log_format_is_decoded() -> None:
    """Airflow 2.10 returns JSON `content` as a repr string of (host, text) tuples."""
    fake = seeded(2)
    raw = "line one\nboom: ValueError"
    original = fake._dag_subresource

    def repr_logs(request, rest):  # type: ignore[no-untyped-def]
        if rest.endswith("/logs/2"):
            return httpx.Response(200, json={"content": repr([("6f617d45c53c", raw)])})
        return original(request, rest)

    fake._dag_subresource = repr_logs  # type: ignore[method-assign]
    log = await live(fake).get_task_log("etl", "r2", "load", 2, max_bytes=10_000)
    assert log.content == raw


async def test_log_tail_truncation() -> None:
    fake = seeded(3)
    fake.logs[("etl", "r2", "load", 2)] = "x" * 5000 + "\nFINAL ERROR"
    log = await live(fake).get_task_log("etl", "r2", "load", 2, max_bytes=100)
    assert log.truncated is True
    assert log.content.endswith("FINAL ERROR")
    assert len(log.content.encode()) <= 100


async def test_missing_log_is_not_fatal() -> None:
    log = await live(seeded(3)).get_task_log("etl", "r2", "nope", 1, max_bytes=100)
    assert log.available is False


def test_tail_text_keeps_short_text() -> None:
    assert tail_text("short", 100) == ("short", False)


# ---------------------------------------------------------------------- mock scenarios


@pytest.fixture
def frozen_clock(monkeypatch: pytest.MonkeyPatch) -> datetime:
    # Bucket b with b % 3 == 1: current run running, previous (b-1, b%3==0) failed.
    b = (int(datetime(2026, 9, 24, 12, tzinfo=UTC).timestamp()) // 300) // 3 * 3 + 1
    now = datetime.fromtimestamp(b * 300 + 30, UTC)
    monkeypatch.setattr(mock_module, "clock", lambda: now)
    return now


async def test_mock_orders_pipeline_failure_scenario(frozen_clock: datetime) -> None:
    mock = MockAirflowAdapter(AdapterConfig(base_url="http://mock"), simulate_latency=False)
    runs = await mock.list_dag_runs("orders_pipeline", since=frozen_clock - timedelta(minutes=20))
    assert [r.state for r in runs] == ["running", "failed", "success", "success"]
    failed = runs[1]
    tis = await mock.list_task_instances("orders_pipeline", failed.run_id)
    assert [(t.task_id, t.state) for t in tis][-2:] == [
        ("load_orders", "failed"),
        ("notify_downstream", "upstream_failed"),
    ]
    log = await mock.get_task_log(
        "orders_pipeline", failed.run_id, "load_orders", 2, max_bytes=65536
    )
    assert "UniqueViolation" in log.content


async def test_mock_is_stable_and_legacy_has_no_recent_success(frozen_clock: datetime) -> None:
    mock = MockAirflowAdapter(AdapterConfig(base_url="http://mock"), simulate_latency=False)
    first = await mock.list_dag_runs("orders_pipeline")
    second = await mock.list_dag_runs("orders_pipeline")
    assert first == second
    assert await mock.list_dag_runs("legacy_inventory_sync") == []
