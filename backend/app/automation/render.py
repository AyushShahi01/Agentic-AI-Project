"""Tiny, safe `{{dotted.key}}` templating for notification titles and messages.

Only looks up keys in a plain dict; no attribute access, expressions, or code execution.
"""

import re
from typing import Any

_PLACEHOLDER = re.compile(r"\{\{\s*([A-Za-z0-9_.]+)\s*\}\}")


def _lookup(values: dict[str, Any], dotted: str) -> Any:
    current: Any = values
    for part in dotted.split("."):
        if not isinstance(current, dict) or part not in current:
            return None
        current = current[part]
    return current


def render(template: str, values: dict[str, Any]) -> str:
    """Replace `{{a.b}}` with values["a"]["b"]; unknown keys render as an empty string."""

    def _sub(match: re.Match[str]) -> str:
        value = _lookup(values, match.group(1))
        return "" if value is None else str(value)

    return _PLACEHOLDER.sub(_sub, template)
