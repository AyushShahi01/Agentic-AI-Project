"""Workflow graph: node catalog (types, ports, config models) and validation.

A workflow graph is `{"nodes": [{id, type, name?, config}], "edges": [{from, port, to}]}`.
Execution starts at the single trigger node and follows the edge for each node's output port;
a port with no edge ends the run. This is also the contract for the Plan 3 canvas.

Pure: Pydantic only, no database or framework imports.
"""

import math
import re
from dataclasses import dataclass, field
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

from app.detection.types import IncidentSeverity, IncidentType
from app.diagnosis.log_classifier import FailureCategory

MAX_NODES = 100
MAX_COORDINATE = 100_000
NODE_ID_PATTERN = r"^[A-Za-z0-9_-]{1,100}$"


class _Config(BaseModel):
    model_config = ConfigDict(extra="forbid")


class NoConfig(_Config):
    pass


class IncidentTriggerConfig(_Config):
    events: list[Literal["opened", "recurred"]] = Field(default=["opened"], min_length=1)
    incident_types: list[IncidentType] = Field(default_factory=list)  # empty = any


class StaleTriggerConfig(_Config):
    minutes: int = Field(default=30, ge=5, le=10080)


class FilterConfig(_Config):
    """All criteria that are set must match; unset (empty/None) criteria are ignored."""

    environments: list[Literal["DEV", "STAGING", "PROD"]] = Field(default_factory=list)
    incident_types: list[IncidentType] = Field(default_factory=list)
    min_severity: IncidentSeverity | None = None
    dag_ids: list[str] = Field(default_factory=list)
    tags_any: list[str] = Field(default_factory=list)
    min_occurrences: int | None = Field(default=None, ge=1)
    max_occurrences: int | None = Field(default=None, ge=1)
    diagnosis_categories: list[FailureCategory] = Field(default_factory=list)


class ApprovalConfig(_Config):
    # Approval is required when the connection environment is listed; empty = always required.
    required_environments: list[Literal["DEV", "STAGING", "PROD"]] = Field(
        default_factory=lambda: ["PROD"]
    )
    timeout_minutes: int = Field(default=60, ge=1, le=10080)


class ClearTasksConfig(_Config):
    include_downstream: bool = True


class SetPausedConfig(_Config):
    paused: bool = True


class VerifyConfig(_Config):
    timeout_minutes: int = Field(default=30, ge=1, le=1440)


class IncidentUpdateConfig(_Config):
    operation: Literal["resolve", "escalate", "acknowledge", "note"]
    note: str | None = Field(default=None, max_length=2000)


class NotifyConfig(_Config):
    channel: Literal["in_app", "webhook"] = "in_app"
    level: Literal["INFO", "WARNING", "CRITICAL"] = "INFO"
    title: str = Field(default="{{incident.title}}", min_length=1, max_length=300)
    message: str = Field(default="", max_length=4000)
    url: str | None = Field(default=None, max_length=1000)

    @field_validator("url")
    @classmethod
    def _http_url(cls, value: str | None) -> str | None:
        if value is not None and not value.startswith(("http://", "https://")):
            raise ValueError("url must start with http:// or https://")
        return value


@dataclass(frozen=True)
class NodeType:
    type: str
    label: str
    category: Literal["trigger", "logic", "diagnosis", "approval", "action", "verify", "output"]
    description: str
    ports: tuple[str, ...]
    config_model: type[_Config] = NoConfig
    port_labels: dict[str, str] = field(default_factory=dict)

    def catalog_entry(self) -> dict[str, Any]:
        return {
            "type": self.type,
            "label": self.label,
            "category": self.category,
            "description": self.description,
            "ports": list(self.ports),
            "port_labels": self.port_labels,
            "config_schema": self.config_model.model_json_schema(),
        }


NODE_TYPES: dict[str, NodeType] = {
    t.type: t
    for t in (
        NodeType(
            "trigger.incident",
            "When an incident opens or recurs",
            "trigger",
            "Starts when detection opens an incident or records it again.",
            ("next",),
            IncidentTriggerConfig,
        ),
        NodeType(
            "trigger.incident_stale",
            "When an incident is ignored",
            "trigger",
            "Starts when an incident stays open and unacknowledged for too long.",
            ("next",),
            StaleTriggerConfig,
        ),
        NodeType(
            "condition.filter",
            "Only if…",
            "logic",
            "Continues on 'true' when every configured criterion matches.",
            ("true", "false"),
            FilterConfig,
            {"true": "matches", "false": "does not match"},
        ),
        NodeType(
            "diagnose.classify_log",
            "Figure out why",
            "diagnosis",
            "Reads the failure log and labels the cause (network glitch, bad data, …).",
            ("next",),
        ),
        NodeType(
            "check.dag_state",
            "Check the DAG",
            "logic",
            "Asks Airflow whether the DAG is paused or already has an active run.",
            ("ready", "paused", "busy"),
            port_labels={"ready": "not paused, idle", "paused": "paused", "busy": "run active"},
        ),
        NodeType(
            "approval.request",
            "Ask a human",
            "approval",
            "Waits for an operator to approve. Auto-approves outside the listed environments.",
            ("approved", "rejected"),
            ApprovalConfig,
        ),
        NodeType(
            "action.clear_failed_tasks",
            "Retry failed tasks",
            "action",
            "Clears the failed tasks of the incident's latest failing run so Airflow reruns them.",
            ("success", "failed"),
            ClearTasksConfig,
        ),
        NodeType(
            "action.trigger_dag_run",
            "Start a new run",
            "action",
            "Triggers a new DAG run.",
            ("success", "failed"),
        ),
        NodeType(
            "action.set_dag_paused",
            "Pause / unpause DAG",
            "action",
            "Pauses (or unpauses) the DAG in Airflow.",
            ("success", "failed"),
            SetPausedConfig,
        ),
        NodeType(
            "verify.run_success",
            "Check it worked",
            "verify",
            "Waits for the run touched by the last action to finish and checks it succeeded.",
            ("success", "failed"),
            VerifyConfig,
        ),
        NodeType(
            "incident.update",
            "Update the incident",
            "output",
            "Resolves, escalates, acknowledges, or adds a note to the incident.",
            ("next",),
            IncidentUpdateConfig,
        ),
        NodeType(
            "notify",
            "Tell someone",
            "output",
            "Sends an in-app notification or a webhook. Placeholders like {{incident.title}}.",
            ("next",),
            NotifyConfig,
        ),
    )
}

TRIGGER_TYPES = frozenset(t for t, nt in NODE_TYPES.items() if nt.category == "trigger")
ACTION_TYPES = frozenset(t for t, nt in NODE_TYPES.items() if nt.category == "action")


class GraphError(ValueError):
    def __init__(self, problems: list[dict[str, str]]) -> None:
        super().__init__("; ".join(p["message"] for p in problems))
        self.problems = problems


@dataclass(frozen=True)
class Node:
    id: str
    type: str
    name: str | None
    config: dict[str, Any]
    position: dict[str, float] | None = None  # canvas layout only; ignored by the engine


@dataclass(frozen=True)
class Graph:
    nodes: dict[str, Node]
    edges: dict[tuple[str, str], str]  # (from, port) -> to
    trigger_id: str

    @property
    def trigger(self) -> Node:
        return self.nodes[self.trigger_id]

    def next_node(self, node_id: str, port: str) -> str | None:
        return self.edges.get((node_id, port))


def _problem(message: str, **where: str) -> dict[str, str]:
    return {"message": message, **where}


def validate_graph(raw: Any) -> Graph:
    """Validate a raw graph and return it with defaults applied. Raises GraphError."""
    if not isinstance(raw, dict):
        raise GraphError([_problem("Graph must be an object with 'nodes' and 'edges'")])
    raw_nodes, raw_edges = raw.get("nodes"), raw.get("edges", [])
    if not isinstance(raw_nodes, list) or not raw_nodes:
        raise GraphError([_problem("Graph needs at least one node")])
    if not isinstance(raw_edges, list):
        raise GraphError([_problem("'edges' must be a list")])
    if len(raw_nodes) > MAX_NODES:
        raise GraphError([_problem(f"At most {MAX_NODES} nodes are allowed")])

    problems: list[dict[str, str]] = []
    nodes: dict[str, Node] = {}
    for index, item in enumerate(raw_nodes):
        if not isinstance(item, dict):
            problems.append(_problem(f"Node #{index + 1} must be an object"))
            continue
        node_id = str(item.get("id", ""))
        if not re.fullmatch(NODE_ID_PATTERN, node_id):
            problems.append(_problem(f"Node #{index + 1} has an invalid id", node=node_id))
            continue
        if node_id in nodes:
            problems.append(_problem(f"Duplicate node id '{node_id}'", node=node_id))
            continue
        node_type = NODE_TYPES.get(str(item.get("type")))
        if node_type is None:
            problems.append(_problem(f"Unknown node type '{item.get('type')}'", node=node_id))
            continue
        try:
            config = node_type.config_model.model_validate(item.get("config") or {})
        except ValidationError as exc:
            for err in exc.errors():
                loc = ".".join(str(p) for p in err["loc"]) or "config"
                problems.append(_problem(f"{loc}: {err['msg']}", node=node_id))
            continue
        position, problem = _position(item.get("position"))
        if problem:
            problems.append(_problem(problem, node=node_id))
            continue
        name = item.get("name")
        nodes[node_id] = Node(
            id=node_id,
            type=node_type.type,
            name=str(name)[:200] if name else None,
            config=config.model_dump(mode="json"),
            position=position,
        )

    raw_ids = {str(n.get("id")) for n in raw_nodes if isinstance(n, dict)}
    edges: dict[tuple[str, str], str] = {}
    for index, item in enumerate(raw_edges):
        label = f"Edge #{index + 1}"
        if not isinstance(item, dict):
            problems.append(_problem(f"{label} must be an object"))
            continue
        src, port, dst = str(item.get("from", "")), str(item.get("port", "")), str(item.get("to"))
        edge = f"{src}.{port}->{dst}"
        if src not in nodes or dst not in nodes:
            if src not in raw_ids or dst not in raw_ids:  # else the node itself was reported
                problems.append(_problem(f"{label} references an unknown node", edge=edge))
            continue
        if port not in NODE_TYPES[nodes[src].type].ports:
            problems.append(
                _problem(f"{label}: node '{src}' has no output port '{port}'", edge=edge)
            )
            continue
        if nodes[dst].type in TRIGGER_TYPES:
            problems.append(_problem(f"{label} cannot point at a trigger", edge=edge))
            continue
        if (src, port) in edges:
            problems.append(_problem(f"{label}: port '{port}' of '{src}' is already connected"))
            continue
        edges[(src, port)] = dst

    triggers = [n.id for n in nodes.values() if n.type in TRIGGER_TYPES]
    if len(triggers) != 1 and not problems:
        problems.append(_problem(f"A workflow needs exactly one trigger (found {len(triggers)})"))
    if problems:
        raise GraphError(problems)

    graph = Graph(nodes=nodes, edges=edges, trigger_id=triggers[0])
    _check_acyclic_and_reachable(graph)
    return graph


def _position(raw: Any) -> tuple[dict[str, float] | None, str | None]:
    if raw is None:
        return None, None
    if not isinstance(raw, dict):
        return None, "position must be an object with x and y"
    try:
        x, y = float(raw["x"]), float(raw["y"])
    except (KeyError, TypeError, ValueError):
        return None, "position needs numeric x and y"
    if not (math.isfinite(x) and math.isfinite(y)) or max(abs(x), abs(y)) > MAX_COORDINATE:
        return None, "position is out of range"
    return {"x": round(x, 1), "y": round(y, 1)}, None


def _check_acyclic_and_reachable(graph: Graph) -> None:
    adjacency: dict[str, list[str]] = {n: [] for n in graph.nodes}
    for (src, _), dst in graph.edges.items():
        adjacency[src].append(dst)

    visiting: set[str] = set()
    done: set[str] = set()

    def visit(node_id: str) -> None:
        if node_id in done:
            return
        if node_id in visiting:
            raise GraphError([_problem("The workflow contains a loop", node=node_id)])
        visiting.add(node_id)
        for nxt in adjacency[node_id]:
            visit(nxt)
        visiting.discard(node_id)
        done.add(node_id)

    visit(graph.trigger_id)
    for node_id in graph.nodes:
        visit(node_id)  # also catches cycles among unreachable nodes

    reachable: set[str] = set()
    stack = [graph.trigger_id]
    while stack:
        current = stack.pop()
        if current not in reachable:
            reachable.add(current)
            stack.extend(adjacency[current])
    unreachable = sorted(set(graph.nodes) - reachable)
    if unreachable:
        raise GraphError(
            [_problem(f"Node '{n}' is not connected to the trigger", node=n) for n in unreachable]
        )


def to_raw(graph: Graph) -> dict[str, Any]:
    """Serialize a validated graph (defaults applied) back to JSON form."""
    return {
        "nodes": [
            {
                "id": n.id,
                "type": n.type,
                **({"name": n.name} if n.name else {}),
                "config": n.config,
                **({"position": n.position} if n.position else {}),
            }
            for n in graph.nodes.values()
        ],
        "edges": [{"from": s, "port": p, "to": d} for (s, p), d in graph.edges.items()],
    }


def catalog() -> list[dict[str, Any]]:
    return [t.catalog_entry() for t in NODE_TYPES.values()]
