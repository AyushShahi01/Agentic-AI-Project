"""Scriptable fake Airflow REST API for httpx.MockTransport."""

import base64
import json
import re
import time
from collections.abc import Callable
from dataclasses import dataclass, field

import httpx
import jwt

_KEY = "fake-airflow-signing-key-0123456789abcdef"


@dataclass
class FakeAirflow:
    major: int = 3  # 2 -> /api/v1 + basic auth; 3 -> /api/v2 + /auth/token
    username: str = "admin"
    password: str = "admin"
    static_token: str = "static-token"
    forbidden: bool = False
    dags: list[dict] = field(
        default_factory=lambda: [
            {
                "dag_id": "etl_a",
                "description": "A",
                "is_paused": False,
                "tags": [{"name": "etl"}],
                "timetable_summary": "@daily",
            },
            {
                "dag_id": "etl_b",
                "description": None,
                "is_paused": True,
                "tags": [],
                "schedule_interval": {"__type": "CronExpression", "value": "0 1 * * *"},
            },
        ]
    )
    token_requests: int = 0
    requests: list[str] = field(default_factory=list)
    # dag_id -> runs in Airflow API shape (any order); see add_run()
    runs: dict[str, list[dict]] = field(default_factory=dict)
    # (dag_id, run_id) -> task instances in API shape
    task_instances: dict[tuple[str, str], list[dict]] = field(default_factory=dict)
    # (dag_id, run_id, task_id, try) -> log text
    logs: dict[tuple[str, str, str, int], str] = field(default_factory=dict)
    last_run_params: dict[str, str] = field(default_factory=dict)
    # write calls: (method, path-after-prefix, json body, query params)
    writes: list[tuple[str, str, dict, dict]] = field(default_factory=list)
    reject_writes: int | None = None  # respond to write calls with this status

    def add_run(
        self,
        dag_id: str,
        run_id: str,
        state: str,
        logical_date: str,
        end_date: str | None = None,
        *,
        failed_task: str | None = None,
        log: str = "",
    ) -> None:
        date_key = "logical_date" if self.major == 3 else "execution_date"
        self.runs.setdefault(dag_id, []).append(
            {
                "dag_run_id": run_id,
                "state": state,
                date_key: logical_date,
                "start_date": logical_date,
                "end_date": end_date or logical_date,
                "run_type": "scheduled",
            }
        )
        tis = [{"task_id": "extract", "state": "success", "try_number": 1}]
        if failed_task:
            tis.append({"task_id": failed_task, "state": "failed", "try_number": 2})
            self.logs[(dag_id, run_id, failed_task, 2)] = log
        self.task_instances[(dag_id, run_id)] = tis

    @property
    def prefix(self) -> str:
        return "/api/v2" if self.major == 3 else "/api/v1"

    def _issue_jwt(self) -> str:
        return jwt.encode({"sub": self.username, "exp": int(time.time()) + 3600}, _KEY, "HS256")

    def _authorized(self, request: httpx.Request) -> bool:
        header = request.headers.get("authorization", "")
        if header == f"Bearer {self.static_token}":
            return True
        if self.major == 3 and header.startswith("Bearer "):
            try:
                return jwt.decode(header[7:], _KEY, algorithms=["HS256"])["sub"] == self.username
            except jwt.PyJWTError:
                return False
        if self.major == 2 and header.startswith("Basic "):
            user, _, pw = base64.b64decode(header[6:]).decode().partition(":")
            return (user, pw) == (self.username, self.password)
        return False

    def handler(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        self.requests.append(f"{request.method} {path}")
        if path == f"{self.prefix}/version":
            return httpx.Response(200, json={"version": f"{self.major}.1.0", "git_version": None})
        if self.major == 3 and path == "/auth/token" and request.method == "POST":
            self.token_requests += 1
            body = json.loads(request.content)
            if (body.get("username"), body.get("password")) != (self.username, self.password):
                return httpx.Response(401, json={"detail": "Invalid credentials"})
            return httpx.Response(201, json={"access_token": self._issue_jwt()})
        if path == f"{self.prefix}/dags":
            if not self._authorized(request):
                return httpx.Response(401, json={"detail": "Unauthorized"})
            if self.forbidden:
                return httpx.Response(403, json={"detail": "Forbidden"})
            limit = int(request.url.params.get("limit", 100))
            offset = int(request.url.params.get("offset", 0))
            return httpx.Response(
                200,
                json={"dags": self.dags[offset : offset + limit], "total_entries": len(self.dags)},
            )
        if path.startswith(f"{self.prefix}/dags/"):
            if not self._authorized(request):
                return httpx.Response(401, json={"detail": "Unauthorized"})
            return self._dag_subresource(request, path[len(f"{self.prefix}/dags/") :])
        return httpx.Response(404, json={"detail": "Not Found"})

    def _dag_subresource(self, request: httpx.Request, rest: str) -> httpx.Response:
        params = request.url.params
        if request.method in ("POST", "PATCH"):
            return self._write(request, rest)
        if "/" not in rest:
            dag = next((d for d in self.dags if d["dag_id"] == rest), None)
            return httpx.Response(200, json=dag) if dag else httpx.Response(404, json={})
        if m := re.fullmatch(r"([^/]+)/dagRuns/([^/]+)", rest):
            run = next(
                (r for r in self.runs.get(m.group(1), []) if r["dag_run_id"] == m.group(2)), None
            )
            return httpx.Response(200, json=run) if run else httpx.Response(404, json={})
        if m := re.fullmatch(r"([^/]+)/dagRuns", rest):
            dag_id = m.group(1)
            self.last_run_params = dict(params)
            date_key = "logical_date" if self.major == 3 else "execution_date"
            runs = sorted(self.runs.get(dag_id, []), key=lambda r: r[date_key], reverse=True)
            if gte := params.get(f"{date_key}_gte"):
                runs = [r for r in runs if r[date_key] >= gte.replace("Z", "+00:00")]
            offset, limit = int(params.get("offset", 0)), int(params.get("limit", 100))
            page = runs[offset : offset + limit]
            return httpx.Response(200, json={"dag_runs": page, "total_entries": len(runs)})
        if m := re.fullmatch(r"([^/]+)/dagRuns/([^/]+)/taskInstances", rest):
            tis = self.task_instances.get((m.group(1), m.group(2)), [])
            return httpx.Response(200, json={"task_instances": tis, "total_entries": len(tis)})
        if m := re.fullmatch(r"([^/]+)/dagRuns/([^/]+)/taskInstances/([^/]+)/logs/(\d+)", rest):
            key = (m.group(1), m.group(2), m.group(3), int(m.group(4)))
            if key not in self.logs:
                return httpx.Response(404, json={"detail": "Log not found"})
            text = self.logs[key]
            if self.major == 3:  # structured log events
                content = [
                    {"timestamp": "t", "level": "info", "event": line} for line in text.splitlines()
                ]
            else:
                content = text
            return httpx.Response(200, json={"content": content, "continuation_token": None})
        return httpx.Response(404, json={"detail": "Not Found"})

    def _write(self, request: httpx.Request, rest: str) -> httpx.Response:
        body = json.loads(request.content or b"{}")
        self.writes.append((request.method, rest, body, dict(request.url.params)))
        if self.reject_writes:
            return httpx.Response(self.reject_writes, json={"detail": "Rejected by fake"})
        if request.method == "PATCH" and "/" not in rest:
            dag = next((d for d in self.dags if d["dag_id"] == rest), None)
            if dag is None:
                return httpx.Response(404, json={})
            dag["is_paused"] = body["is_paused"]
            return httpx.Response(200, json=dag)
        if m := re.fullmatch(r"([^/]+)/clearTaskInstances", rest):
            tis = self.task_instances.get((m.group(1), body["dag_run_id"]), [])
            cleared = [t for t in tis if t["state"] in ("failed", "upstream_failed")]
            for run in self.runs.get(m.group(1), []):
                if run["dag_run_id"] == body["dag_run_id"]:
                    run["state"] = "queued"
            return httpx.Response(200, json={"task_instances": cleared, "total_entries": 0})
        if m := re.fullmatch(r"([^/]+)/dagRuns", rest):
            date_key = "logical_date" if self.major == 3 else "execution_date"
            run = {
                "dag_run_id": f"manual__{len(self.writes)}",
                "state": "queued",
                date_key: "2026-09-24T12:00:00+00:00",
                "run_type": "manual",
            }
            self.runs.setdefault(m.group(1), []).append(run)
            return httpx.Response(200, json=run)
        return httpx.Response(404, json={"detail": "Not Found"})

    def transport(self) -> httpx.MockTransport:
        return httpx.MockTransport(self.handler)


def static(
    status: int, *, content: str = "", headers: dict | None = None
) -> Callable[[httpx.Request], httpx.Response]:
    def _handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(status, text=content, headers=headers or {})

    return _handler


def raising(exc: Exception) -> Callable[[httpx.Request], httpx.Response]:
    def _handler(_: httpx.Request) -> httpx.Response:
        raise exc

    return _handler
