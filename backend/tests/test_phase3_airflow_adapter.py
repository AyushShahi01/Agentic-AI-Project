"""Phase 3a: the live and mock Airflow adapters (no database, no HTTP API)."""

import ssl

import httpx
import pytest

from app.orchestration.airflow.base import (
    AdapterConfig,
    AirflowAdapterError,
    ApiVersion,
    AuthType,
    ConnectionStatus,
    normalize_base_url,
)
from app.orchestration.airflow.client import LiveAirflowAdapter
from app.orchestration.airflow.mock import MOCK_DAGS, MockAirflowAdapter
from tests.fake_airflow import FakeAirflow, raising, static

BASE = "http://airflow.test:8080"


def adapter(transport: httpx.MockTransport | httpx.AsyncBaseTransport, **config: object):
    cfg = {"base_url": BASE, "auth_type": AuthType.BASIC, "username": "admin", "secret": "admin"}
    cfg.update(config)
    return LiveAirflowAdapter(AdapterConfig(**cfg), transport=transport)  # type: ignore[arg-type]


# ---------------------------------------------------------------------- version detection & auth


async def test_airflow3_detected_and_uses_token_auth() -> None:
    fake = FakeAirflow(major=3)
    result = await adapter(fake.transport()).probe()
    assert result.status == ConnectionStatus.HEALTHY
    assert result.api_version == ApiVersion.V2
    assert result.airflow_version == "3.1.0"
    assert result.latency_ms is not None
    assert fake.token_requests == 1


async def test_airflow3_token_is_cached() -> None:
    fake = FakeAirflow(major=3)
    await adapter(fake.transport()).probe()
    await adapter(fake.transport()).list_dags()
    assert fake.token_requests == 1


async def test_airflow2_detected_via_v1_fallback_with_basic_auth() -> None:
    fake = FakeAirflow(major=2)
    result = await adapter(fake.transport()).probe()
    assert result.status == ConnectionStatus.HEALTHY
    assert result.api_version == ApiVersion.V1
    assert "GET /api/v2/version" in fake.requests  # tried v2 first
    assert fake.token_requests == 0


@pytest.mark.parametrize("major", [2, 3])
async def test_static_bearer_token_auth(major: int) -> None:
    fake = FakeAirflow(major=major)
    result = await adapter(
        fake.transport(), auth_type=AuthType.TOKEN, username=None, secret="static-token"
    ).probe()
    assert result.ok


# ---------------------------------------------------------------------- error matrix


@pytest.mark.parametrize("major", [2, 3])
async def test_bad_credentials_unauthorized(major: int) -> None:
    result = await adapter(FakeAirflow(major=major).transport(), secret="wrong").probe()
    assert result.status == ConnectionStatus.UNAUTHORIZED
    assert "Authentication failed" in result.message


async def test_forbidden_distinct_from_unauthorized() -> None:
    result = await adapter(FakeAirflow(major=3, forbidden=True).transport()).probe()
    assert result.status == ConnectionStatus.FORBIDDEN
    assert "lack permission" in result.message


async def test_connect_error_unreachable() -> None:
    transport = httpx.MockTransport(raising(httpx.ConnectError("connection refused")))
    result = await adapter(transport).probe()
    assert result.status == ConnectionStatus.UNREACHABLE
    assert BASE in result.message


async def test_timeout_unreachable() -> None:
    transport = httpx.MockTransport(raising(httpx.ReadTimeout("timed out")))
    assert (await adapter(transport).probe()).status == ConnectionStatus.UNREACHABLE


async def test_tls_error() -> None:
    err = httpx.ConnectError("[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed")
    err.__cause__ = ssl.SSLCertVerificationError("certificate verify failed")
    result = await adapter(httpx.MockTransport(raising(err))).probe()
    assert result.status == ConnectionStatus.TLS_ERROR


async def test_not_airflow_404_on_both_versions() -> None:
    transport = httpx.MockTransport(static(404, content="nope"))
    result = await adapter(transport).probe()
    assert result.status == ConnectionStatus.INVALID_ENDPOINT
    assert "No Airflow REST API" in result.message


async def test_html_login_page_is_invalid_endpoint() -> None:
    html = "<html><body>Please sign in</body></html>"
    transport = httpx.MockTransport(
        static(200, content=html, headers={"content-type": "text/html"})
    )
    result = await adapter(transport).probe()
    assert result.status == ConnectionStatus.INVALID_ENDPOINT
    assert "web page" in result.message


async def test_redirect_to_sso_is_invalid_endpoint() -> None:
    transport = httpx.MockTransport(
        static(302, headers={"location": "https://sso.example.com/login"})
    )
    result = await adapter(transport).probe()
    assert result.status == ConnectionStatus.INVALID_ENDPOINT
    assert "redirect" in result.message


async def test_server_error_is_airflow_error() -> None:
    transport = httpx.MockTransport(static(500, content="boom"))
    result = await adapter(transport).probe()
    assert result.status == ConnectionStatus.AIRFLOW_ERROR
    assert "500" in result.message


# ---------------------------------------------------------------------- DAG listing


@pytest.mark.parametrize("major", [2, 3])
async def test_list_dags_parses_both_versions(major: int) -> None:
    dags = await adapter(FakeAirflow(major=major).transport()).list_dags()
    by_id = {d.dag_id: d for d in dags}
    assert by_id["etl_a"].schedule_summary == "@daily"
    assert by_id["etl_a"].tags == ["etl"]
    assert by_id["etl_b"].schedule_summary == "0 1 * * *"
    assert by_id["etl_b"].is_paused is True


async def test_list_dags_paginates() -> None:
    fake = FakeAirflow(major=3)
    fake.dags = [{"dag_id": f"dag_{i:03d}", "tags": []} for i in range(250)]
    dags = await adapter(fake.transport()).list_dags()
    assert len(dags) == 250
    assert sum(1 for r in fake.requests if r.endswith("/dags")) == 3


async def test_list_dags_raises_adapter_error() -> None:
    with pytest.raises(AirflowAdapterError) as info:
        await adapter(FakeAirflow(major=3).transport(), secret="wrong").list_dags()
    assert info.value.result.status == ConnectionStatus.UNAUTHORIZED


# ---------------------------------------------------------------------- mock adapter


async def test_mock_adapter_healthy() -> None:
    mock = MockAirflowAdapter(AdapterConfig(base_url="http://mock"), simulate_latency=False)
    result = await mock.probe()
    assert result.ok and result.api_version == ApiVersion.V2
    assert len(await mock.list_dags()) == len(MOCK_DAGS)


async def test_mock_adapter_simulates_failures() -> None:
    mock = MockAirflowAdapter(
        AdapterConfig(base_url="http://mock?simulate=unauthorized"), simulate_latency=False
    )
    assert (await mock.probe()).status == ConnectionStatus.UNAUTHORIZED
    with pytest.raises(AirflowAdapterError):
        await mock.list_dags()


# ---------------------------------------------------------------------- URL normalization


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("http://host:8080/", "http://host:8080"),
        ("http://host:8080/api/v1", "http://host:8080"),
        ("https://host/airflow/api/v2/", "https://host/airflow"),
    ],
)
def test_normalize_base_url(raw: str, expected: str) -> None:
    assert normalize_base_url(raw) == expected


@pytest.mark.parametrize(
    "raw", ["ftp://host", "http://", "http://user:pw@host", "http://host?x=1", "file:///etc/passwd"]
)
def test_normalize_base_url_rejects(raw: str) -> None:
    with pytest.raises(ValueError):
        normalize_base_url(raw)
