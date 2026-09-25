import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta

import jwt
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.core.exceptions import AuthenticationError
from app.core.rate_limit import LoginRateLimiter
from app.core.security import (
    DUMMY_PASSWORD_HASH,
    create_access_token,
    create_refresh_token,
    decode_token,
    verify_password,
)
from app.db.base import utcnow
from app.models.user import RefreshToken, User
from app.services import audit_service, user_service

INVALID_CREDENTIALS = "Incorrect email or password"


@dataclass(frozen=True)
class IssuedTokens:
    user: User
    access_token: str
    expires_in: int
    refresh_token: str
    refresh_expires_at: datetime


def _issue_tokens(db: Session, user: User) -> tuple[IssuedTokens, RefreshToken]:
    expires_at = utcnow() + timedelta(days=get_settings().REFRESH_TOKEN_EXPIRE_DAYS)
    record = RefreshToken(id=uuid.uuid4(), user_id=user.id, expires_at=expires_at)
    db.add(record)
    access_token, expires_in = create_access_token(user.id, user.role)
    tokens = IssuedTokens(
        user=user,
        access_token=access_token,
        expires_in=expires_in,
        refresh_token=create_refresh_token(user.id, record.id, expires_at),
        refresh_expires_at=expires_at,
    )
    return tokens, record


def login(
    db: Session,
    *,
    email: str,
    password: str,
    limiter: LoginRateLimiter,
    ip_address: str | None,
) -> IssuedTokens:
    key = limiter.key(email, ip_address)
    limiter.check(key)

    user = user_service.get_by_email(db, email)
    # Always run a hash verification so unknown emails take the same time as known ones.
    password_ok = verify_password(password, user.hashed_password if user else DUMMY_PASSWORD_HASH)

    if user is None or not password_ok or not user.is_active:
        limiter.record_failure(key)
        reason = (
            "unknown_email" if user is None else "bad_password" if not password_ok else "inactive"
        )
        audit_service.record(
            db,
            action="auth.login_failed",
            entity_type="user",
            entity_id=user.id if user else None,
            details={"email": email.strip().lower(), "reason": reason},
            ip_address=ip_address,
        )
        db.commit()
        raise AuthenticationError(INVALID_CREDENTIALS)

    limiter.reset(key)
    user.last_login_at = utcnow()
    tokens, _ = _issue_tokens(db, user)
    audit_service.record(
        db,
        action="auth.login_success",
        entity_type="user",
        entity_id=user.id,
        actor=user,
        ip_address=ip_address,
    )
    db.commit()
    return tokens


def _load_refresh_record(db: Session, raw_token: str | None) -> RefreshToken:
    if not raw_token:
        raise AuthenticationError("Missing refresh token")
    try:
        payload = decode_token(raw_token, "refresh")
        jti = uuid.UUID(payload["jti"])
    except (jwt.InvalidTokenError, KeyError, ValueError) as exc:
        raise AuthenticationError("Invalid refresh token") from exc
    record = db.get(RefreshToken, jti)
    if record is None or str(record.user_id) != payload["sub"]:
        raise AuthenticationError("Invalid refresh token")
    return record


def _revoke_chain(db: Session, start: RefreshToken) -> None:
    """Revoke a token and every token issued from it by rotation."""
    now = utcnow()
    current: RefreshToken | None = start
    seen: set[uuid.UUID] = set()
    while current is not None and current.id not in seen:
        seen.add(current.id)
        if current.revoked_at is None:
            current.revoked_at = now
        current = db.get(RefreshToken, current.replaced_by) if current.replaced_by else None


def refresh(db: Session, raw_token: str | None, *, ip_address: str | None) -> IssuedTokens:
    record = _load_refresh_record(db, raw_token)

    if record.revoked_at is not None:
        # A rotated-out token was presented again: assume theft and kill the whole chain.
        _revoke_chain(db, record)
        audit_service.record(
            db,
            action="auth.refresh_reuse_detected",
            entity_type="user",
            entity_id=record.user_id,
            details={"refresh_token_id": str(record.id)},
            ip_address=ip_address,
        )
        db.commit()
        raise AuthenticationError("Refresh token has been revoked")

    if record.expires_at <= utcnow():
        raise AuthenticationError("Refresh token has expired")

    user = db.get(User, record.user_id)
    if user is None or not user.is_active:
        raise AuthenticationError("User not found or inactive")

    tokens, new_record = _issue_tokens(db, user)
    record.revoked_at = utcnow()
    record.replaced_by = new_record.id
    db.commit()
    return tokens


def logout(db: Session, raw_token: str | None, *, ip_address: str | None) -> None:
    """Revoke the presented refresh token. Always succeeds (idempotent)."""
    try:
        record = _load_refresh_record(db, raw_token)
    except AuthenticationError:
        return
    if record.revoked_at is None:
        record.revoked_at = utcnow()
    user = db.get(User, record.user_id)
    audit_service.record(
        db,
        action="auth.logout",
        entity_type="user",
        entity_id=record.user_id,
        actor=user,
        ip_address=ip_address,
    )
    db.commit()
