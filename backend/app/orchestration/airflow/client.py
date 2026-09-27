"""Live Airflow REST adapter supporting Airflow 2.x (/api/v1) and 3.x (/api/v2)."""

import ast
import hashlib
import ssl
import time
from datetime import UTC, datetime
from typing import Any
from urllib.parse import quote

import httpx
import jwt

from app.orchestration.airflow.base import (
    AdapterConfig,
    AirflowAdapterError,
    AirflowDagRun,
    AirflowDagSummary,
    AirflowTaskInstance,
    ApiVersion,
    AuthType,
    ConnectionStatus,
    ConnectionTestResult,
    TaskLog,
    describe_status,
    tail_text,
)

_DAG_PAGE_SIZE = 100
_MAX_DAGS = 10_000
_TOKEN_REFRESH_MARGIN_SECONDS = 60
_DEFAULT_TOKEN_TTL_SECONDS = 300

# Airflow 3 JWTs keyed by (base_url, username, secret digest) -> (token, expires_at monotonic)
# (A race between concurrent probes just fetches two tokens; no lock needed.)
_token_cache: dict[tuple[str, str, str], tuple[str, float]] = {}


def clear_token_cache() -> None:
    _token_cache.clear()


class LiveAirflowAdapter:
    def __init__(
        self, config: AdapterConfig, *, transport: httpx.AsyncBaseTransport | None = None
    ) -> None:
        self.config = config
        self._transport = transport

    # ------------------------------------------------------------------ public API

    async def probe(self) -> ConnectionTestResult:
        started = time.perf_counter()
        try:
            async with self._client() as client:
                # Always re-detect on probe: the server may have been upgraded.
                api_version, airflow_version = await self._detect_version(client, use_stored=False)
                headers = await self._auth_headers(client, api_version)
                response = await self._request(
                    client, "GET", f"/api/{api_version}/dags", headers=headers, params={"limit": 1}
                )
                self._check(response)
                if airflow_version is None:
                    airflow_version = await self._fetch_version(client, api_version, headers)
        except AirflowAdapterError as exc:
            return _with_latency(exc.result, started)

        latency = _elapsed_ms(started)
        return ConnectionTestResult(
            status=ConnectionStatus.HEALTHY,
            message=describe_status(
                ConnectionStatus.HEALTHY,
                self.config.base_url,
                airflow_version=airflow_version,
                latency_ms=latency,
            ),
            latency_ms=latency,
            api_version=api_version,
            airflow_version=airflow_version,
        )

    async def list_dags(self) -> list[AirflowDagSummary]:
        async with self._client() as client:
            api_version, _ = await self._detect_version(client)
            headers = await self._auth_headers(client, api_version)

            dags: list[AirflowDagSummary] = []
            offset = 0
            while offset < _MAX_DAGS:
                response = await self._request(
                    client,
                    "GET",
                    f"/api/{api_version}/dags",
                    headers=headers,
                    params={"limit": _DAG_PAGE_SIZE, "offset": offset, "order_by": "dag_id"},
                )
                payload = self._check(response)
                page = payload.get("dags") or []
                dags.extend(_parse_dag(item) for item in page)
                total = payload.get("total_entries", len(dags))
                offset += len(page)
                if not page or offset >= total:
                    break
            return dags

    async def list_dag_runs(
        self, dag_id: str, *, since: datetime | None = None, limit: int = 100
    ) -> list[AirflowDagRun]:
        async with self._client() as client:
            api_version, _ = await self._detect_version(client)
            headers = await self._auth_headers(client, api_version)
            date_field = "logical_date" if api_version == ApiVersion.V2 else "execution_date"
            params: dict[str, Any] = {"limit": min(limit, 100), "order_by": f"-{date_field}"}
            if since is not None:
                params[f"{date_field}_gte"] = _format_date(since)

            runs: list[AirflowDagRun] = []
            offset = 0
            while len(runs) < limit:
                params["offset"] = offset
                response = await self._request(
                    client,
                    "GET",
                    f"/api/{api_version}/dags/{_q(dag_id)}/dagRuns",
                    headers=headers,
                    params=params,
                )
                payload = self._check(response)
                page = payload.get("dag_runs") or []
                runs.extend(_parse_run(item) for item in page)
                offset += len(page)
                if not page or offset >= payload.get("total_entries", offset):
                    break
            return runs[:limit]

    async def list_task_instances(self, dag_id: str, run_id: str) -> list[AirflowTaskInstance]:
        async with self._client() as client:
            api_version, _ = await self._detect_version(client)
            headers = await self._auth_headers(client, api_version)
            instances: list[AirflowTaskInstance] = []
            offset = 0
            while offset < _MAX_DAGS:
                response = await self._request(
                    client,
                    "GET",
                    f"/api/{api_version}/dags/{_q(dag_id)}/dagRuns/{_q(run_id)}/taskInstances",
                    headers=headers,
                    params={"limit": 100, "offset": offset},
                )
                payload = self._check(response)
                page = payload.get("task_instances") or []
                instances.extend(_parse_task_instance(item) for item in page)
                offset += len(page)
                if not page or offset >= payload.get("total_entries", offset):
                    break
            return instances

    async def get_task_log(
        self, dag_id: str, run_id: str, task_id: str, try_number: int, *, max_bytes: int
    ) -> TaskLog:
        async with self._client() as client:
            api_version, _ = await self._detect_version(client)
            headers = await self._auth_headers(client, api_version)
            response = await self._request(
                client,
                "GET",
                f"/api/{api_version}/dags/{_q(dag_id)}/dagRuns/{_q(run_id)}"
                f"/taskInstances/{_q(task_id)}/logs/{int(try_number)}",
                headers=headers,
                params={"full_content": "true"},
            )
            if response.status_code == 404:
                return TaskLog(content="", available=False)
            payload = self._check(response)
            content, truncated = tail_text(_flatten_log(payload.get("content")), max_bytes)
            return TaskLog(content=content, truncated=truncated)

    # ------------------------------------------------------------------ write path

    async def get_dag(self, dag_id: str) -> AirflowDagSummary | None:
        async with self._client() as client:
            api_version, _ = await self._detect_version(client)
            headers = await self._auth_headers(client, api_version)
            response = await self._request(
                client, "GET", f"/api/{api_version}/dags/{_q(dag_id)}", headers=headers
            )
            if response.status_code == 404:
                return None
            return _parse_dag(self._check(response))

    async def get_dag_run(self, dag_id: str, run_id: str) -> AirflowDagRun | None:
        async with self._client() as client:
            api_version, _ = await self._detect_version(client)
            headers = await self._auth_headers(client, api_version)
            response = await self._request(
                client,
                "GET",
                f"/api/{api_version}/dags/{_q(dag_id)}/dagRuns/{_q(run_id)}",
                headers=headers,
            )
            if response.status_code == 404:
                return None
            return _parse_run(self._check(response))

    async def clear_task_instances(
        self, dag_id: str, run_id: str, *, only_failed: bool = True, include_downstream: bool = True
    ) -> list[str]:
        async with self._client() as client:
            api_version, _ = await self._detect_version(client)
            headers = await self._auth_headers(client, api_version)
            response = await self._request(
                client,
                "POST",
                f"/api/{api_version}/dags/{_q(dag_id)}/clearTaskInstances",
                headers=headers,
                json={
                    "dry_run": False,
                    "dag_run_id": run_id,
                    "only_failed": only_failed,
                    "include_downstream": include_downstream,
                    "reset_dag_runs": True,
                },
            )
            payload = self._check_write(response)
            return [
                str(item.get("task_id"))
                for item in payload.get("task_instances") or []
                if item.get("task_id")
            ]

    async def trigger_dag_run(
        self, dag_id: str, *, note: str | None = None, conf: dict[str, Any] | None = None
    ) -> AirflowDagRun:
        async with self._client() as client:
            api_version, _ = await self._detect_version(client)
            headers = await self._auth_headers(client, api_version)
            body: dict[str, Any] = {"conf": conf or {}}
            if api_version == ApiVersion.V2:
                body["logical_date"] = None  # required field in Airflow 3; null = "now"
            if note:
                body["note"] = note[:1000]
            response = await self._request(
                client,
                "POST",
                f"/api/{api_version}/dags/{_q(dag_id)}/dagRuns",
                headers=headers,
                json=body,
            )
            return _parse_run(self._check_write(response))

    async def set_dag_paused(self, dag_id: str, paused: bool) -> None:
        async with self._client() as client:
            api_version, _ = await self._detect_version(client)
            headers = await self._auth_headers(client, api_version)
            response = await self._request(
                client,
                "PATCH",
                f"/api/{api_version}/dags/{_q(dag_id)}",
                headers=headers,
                params={"update_mask": "is_paused"},
                json={"is_paused": paused},
            )
            self._check_write(response)

    # ------------------------------------------------------------------ internals

    def _client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(
            base_url=self.config.base_url,
            timeout=httpx.Timeout(self.config.read_timeout, connect=self.config.connect_timeout),
            follow_redirects=False,
            transport=self._transport,
            headers={"Accept": "application/json"},
        )

    def _fail(
        self,
        status: ConnectionStatus,
        *,
        http_status: int | None = None,
        message: str | None = None,
    ) -> AirflowAdapterError:
        return AirflowAdapterError(
            ConnectionTestResult(
                status=status,
                message=message
                or describe_status(status, self.config.base_url, http_status=http_status),
            )
        )

    async def _request(
        self, client: httpx.AsyncClient, method: str, url: str, **kwargs: Any
    ) -> httpx.Response:
        try:
            return await client.request(method, url, **kwargs)
        except httpx.TimeoutException as exc:
            raise self._fail(ConnectionStatus.UNREACHABLE) from exc
        except httpx.TransportError as exc:
            if _is_tls_error(exc):
                raise self._fail(ConnectionStatus.TLS_ERROR) from exc
            raise self._fail(ConnectionStatus.UNREACHABLE) from exc

    def _check(self, response: httpx.Response) -> dict[str, Any]:
        """Map a non-success or non-JSON response to a failure; return the JSON body."""
        code = response.status_code
        if response.is_redirect or 300 <= code < 400:
            raise self._fail(
                ConnectionStatus.INVALID_ENDPOINT,
                message=(
                    "The URL returned a redirect instead of the API. A proxy or SSO may be in "
                    "front of Airflow, or the base URL is wrong."
                ),
            )
        if code == 401:
            raise self._fail(ConnectionStatus.UNAUTHORIZED)
        if code == 403:
            raise self._fail(ConnectionStatus.FORBIDDEN)
        if code == 404:
            raise self._fail(ConnectionStatus.INVALID_ENDPOINT)
        if code >= 400:
            raise self._fail(ConnectionStatus.AIRFLOW_ERROR, http_status=code)
        body = _json_or_none(response)
        if not isinstance(body, dict):
            raise self._fail(
                ConnectionStatus.INVALID_ENDPOINT,
                message=(
                    "The URL returned a web page instead of the Airflow API. A proxy or SSO may "
                    "be in front of Airflow."
                ),
            )
        return body

    def _check_write(self, response: httpx.Response) -> dict[str, Any]:
        """Like _check, but surfaces Airflow's own message for 400/409 on write calls."""
        if response.status_code in (400, 409, 422):
            body = _json_or_none(response)
            detail = body.get("detail") or body.get("title") if isinstance(body, dict) else None
            raise self._fail(
                ConnectionStatus.AIRFLOW_ERROR,
                message=f"Airflow rejected the request ({response.status_code}): "
                f"{str(detail or response.text)[:300]}",
            )
        return self._check(response)

    async def _detect_version(
        self, client: httpx.AsyncClient, *, use_stored: bool = True
    ) -> tuple[ApiVersion, str | None]:
        """Try /api/v2 (Airflow 3) then /api/v1 (Airflow 2). Returns (api_version, version)."""
        if use_stored and self.config.api_version:
            candidates = [self.config.api_version]
        else:
            candidates = [ApiVersion.V2, ApiVersion.V1]
        last_404: httpx.Response | None = None
        for api_version in candidates:
            response = await self._request(client, "GET", f"/api/{api_version}/version")
            if response.status_code == 404:
                last_404 = response
                continue
            if response.status_code in (401, 403):
                return api_version, None  # API exists but version endpoint needs auth
            body = self._check(response)
            return api_version, body.get("version")
        assert last_404 is not None
        self._check(last_404)
        raise AssertionError("unreachable")

    async def _fetch_version(
        self, client: httpx.AsyncClient, api_version: ApiVersion, headers: dict[str, str]
    ) -> str | None:
        response = await self._request(
            client, "GET", f"/api/{api_version}/version", headers=headers
        )
        if response.status_code != 200:
            return None
        body = _json_or_none(response)
        return body.get("version") if isinstance(body, dict) else None

    async def _auth_headers(
        self, client: httpx.AsyncClient, api_version: ApiVersion
    ) -> dict[str, str]:
        cfg = self.config
        if cfg.auth_type == AuthType.NONE:
            return {}
        if cfg.auth_type == AuthType.TOKEN:
            return {"Authorization": f"Bearer {cfg.secret or ''}"}
        # BASIC
        if api_version == ApiVersion.V1:
            basic = httpx.BasicAuth(cfg.username or "", cfg.secret or "")
            request = next(basic.auth_flow(httpx.Request("GET", cfg.base_url)))
            return {"Authorization": request.headers["Authorization"]}
        token = await self._airflow3_token(client)
        return {"Authorization": f"Bearer {token}"}

    async def _airflow3_token(self, client: httpx.AsyncClient) -> str:
        cfg = self.config
        digest = hashlib.sha256((cfg.secret or "").encode()).hexdigest()
        key = (cfg.base_url, cfg.username or "", digest)
        cached = _token_cache.get(key)
        if cached and cached[1] - _TOKEN_REFRESH_MARGIN_SECONDS > time.monotonic():
            return cached[0]

        response = await self._request(
            client,
            "POST",
            "/auth/token",
            json={"username": cfg.username, "password": cfg.secret},
        )
        if response.status_code == 404:
            raise self._fail(
                ConnectionStatus.INVALID_ENDPOINT,
                message=(
                    "Airflow 3 did not expose /auth/token. Use TOKEN auth with a JWT, or check "
                    "the configured auth manager."
                ),
            )
        body = self._check(response)
        token = body.get("access_token")
        if not token:
            raise self._fail(
                ConnectionStatus.AIRFLOW_ERROR, message="Airflow /auth/token returned no token."
            )
        _token_cache[key] = (token, time.monotonic() + _token_ttl(token))
        return token


# ---------------------------------------------------------------------- helpers


def _elapsed_ms(started: float) -> int:
    return int((time.perf_counter() - started) * 1000)


def _with_latency(result: ConnectionTestResult, started: float) -> ConnectionTestResult:
    return ConnectionTestResult(
        status=result.status,
        message=result.message,
        latency_ms=_elapsed_ms(started),
        api_version=result.api_version,
        airflow_version=result.airflow_version,
    )


def _json_or_none(response: httpx.Response) -> Any:
    if "json" not in response.headers.get("content-type", ""):
        return None
    try:
        return response.json()
    except ValueError:
        return None


def _is_tls_error(exc: BaseException) -> bool:
    current: BaseException | None = exc
    while current is not None:
        if isinstance(current, ssl.SSLError):
            return True
        current = current.__cause__ or current.__context__
    text = str(exc).upper()
    return "CERTIFICATE_VERIFY_FAILED" in text or "SSL" in text


def _token_ttl(token: str) -> float:
    try:
        claims = jwt.decode(token, options={"verify_signature": False})
        exp = float(claims["exp"])
        return max(exp - time.time(), 0)
    except (jwt.PyJWTError, KeyError, TypeError, ValueError):
        return _DEFAULT_TOKEN_TTL_SECONDS


def _q(value: str) -> str:
    return quote(value, safe="")


def _format_date(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _parse_date(value: Any) -> datetime | None:
    if not value or not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _parse_run(item: dict[str, Any]) -> AirflowDagRun:
    return AirflowDagRun(
        run_id=item.get("dag_run_id") or item.get("run_id") or "",
        state=(item.get("state") or "unknown").lower(),
        logical_date=_parse_date(
            item.get("logical_date") or item.get("execution_date") or item.get("run_after")
        ),
        start_date=_parse_date(item.get("start_date")),
        end_date=_parse_date(item.get("end_date")),
        run_type=item.get("run_type"),
    )


def _parse_task_instance(item: dict[str, Any]) -> AirflowTaskInstance:
    return AirflowTaskInstance(
        task_id=item.get("task_id") or "",
        state=(item.get("state") or None),
        try_number=int(item.get("try_number") or 1),
        start_date=_parse_date(item.get("start_date")),
        end_date=_parse_date(item.get("end_date")),
        operator=item.get("operator") or item.get("operator_name"),
    )


def _flatten_log(content: Any) -> str:
    """Airflow 2 returns a string; Airflow 3 returns structured events (or host/text pairs)."""
    if content is None:
        return ""
    if isinstance(content, str):
        # Airflow 2.x JSON logs are a Python repr string: "[('hostname', 'log text')]".
        if content.startswith("[("):
            try:
                content = ast.literal_eval(content)
            except (ValueError, SyntaxError):
                return content
        else:
            return content
    lines: list[str] = []
    for item in content if isinstance(content, list) else [content]:
        if isinstance(item, dict):
            prefix = " ".join(str(item[k]) for k in ("timestamp", "level", "logger") if item.get(k))
            event = item.get("event", "")
            lines.append(f"[{prefix}] {event}" if prefix else str(event))
        elif isinstance(item, list | tuple) and item:
            lines.append(str(item[-1]))  # legacy [(hostname, text)] format
        else:
            lines.append(str(item))
    return "\n".join(lines)


def _parse_dag(item: dict[str, Any]) -> AirflowDagSummary:
    schedule = item.get("timetable_summary") or item.get("timetable_description")
    if not schedule:
        interval = item.get("schedule_interval")
        if isinstance(interval, dict):
            schedule = interval.get("value")
        elif interval:
            schedule = str(interval)
    tags = [
        tag.get("name") if isinstance(tag, dict) else str(tag) for tag in item.get("tags") or []
    ]
    return AirflowDagSummary(
        dag_id=item["dag_id"],
        description=item.get("description"),
        schedule_summary=str(schedule)[:200] if schedule else None,
        is_paused=item.get("is_paused"),
        tags=[t for t in tags if t],
    )
