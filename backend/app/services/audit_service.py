from typing import Any

from sqlalchemy.orm import Session

from app.models.audit_log import ActorType, AuditLog
from app.models.user import User

_SENSITIVE_KEY_PARTS = ("password", "secret", "token", "authorization")
REDACTED = "***"


def redact(value: Any) -> Any:
    """Recursively mask values under sensitive-looking keys (flags like booleans are kept)."""
    if isinstance(value, dict):
        return {
            k: REDACTED
            if any(part in str(k).lower() for part in _SENSITIVE_KEY_PARTS)
            and not isinstance(v, bool | None)
            else redact(v)
            for k, v in value.items()
        }
    if isinstance(value, list):
        return [redact(v) for v in value]
    return value


def record(
    db: Session,
    *,
    action: str,
    entity_type: str,
    entity_id: object | None = None,
    actor: User | None = None,
    details: dict[str, Any] | None = None,
    ip_address: str | None = None,
) -> AuditLog:
    """Add an audit entry to the session. The caller's commit persists it with the change."""
    entry = AuditLog(
        actor_user_id=actor.id if actor else None,
        actor_type=ActorType.USER if actor else ActorType.SYSTEM,
        action=action,
        entity_type=entity_type,
        entity_id=str(entity_id) if entity_id is not None else None,
        details=redact(details or {}),
        ip_address=ip_address,
    )
    db.add(entry)
    return entry
