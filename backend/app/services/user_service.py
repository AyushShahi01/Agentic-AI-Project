import uuid

from sqlalchemy import func, select, update
from sqlalchemy.orm import Session

from app.core.exceptions import ConflictError, NotFoundError
from app.core.security import hash_password
from app.db.base import utcnow
from app.models.user import RefreshToken, User, UserRole
from app.schemas.user import UserCreate, UserUpdate
from app.services import audit_service


def get_by_email(db: Session, email: str) -> User | None:
    return db.scalar(select(User).where(User.email == email.strip().lower()))


def get_user(db: Session, user_id: uuid.UUID) -> User:
    user = db.get(User, user_id)
    if user is None:
        raise NotFoundError("User not found")
    return user


def list_users(
    db: Session, *, role: UserRole | None, limit: int, offset: int
) -> tuple[list[User], int]:
    query = select(User)
    if role is not None:
        query = query.where(User.role == role)
    total = db.scalar(select(func.count()).select_from(query.subquery())) or 0
    items = db.scalars(query.order_by(User.created_at).limit(limit).offset(offset)).all()
    return list(items), total


def create_user(
    db: Session, data: UserCreate, *, actor: User | None, ip_address: str | None = None
) -> User:
    if get_by_email(db, data.email):
        raise ConflictError("A user with this email already exists")
    user = User(
        email=data.email.lower(),
        full_name=data.full_name,
        hashed_password=hash_password(data.password),
        role=data.role,
        is_active=True,
    )
    db.add(user)
    db.flush()
    audit_service.record(
        db,
        action="user.create",
        entity_type="user",
        entity_id=user.id,
        actor=actor,
        details={"email": user.email, "role": user.role},
        ip_address=ip_address,
    )
    db.commit()
    return user


def _other_active_admins(db: Session, user_id: uuid.UUID) -> int:
    return (
        db.scalar(
            select(func.count())
            .select_from(User)
            .where(User.role == UserRole.ADMIN, User.is_active.is_(True), User.id != user_id)
        )
        or 0
    )


def revoke_all_refresh_tokens(db: Session, user_id: uuid.UUID) -> None:
    db.execute(
        update(RefreshToken)
        .where(RefreshToken.user_id == user_id, RefreshToken.revoked_at.is_(None))
        .values(revoked_at=utcnow())
    )


def update_user(
    db: Session,
    user_id: uuid.UUID,
    data: UserUpdate,
    *,
    actor: User,
    ip_address: str | None = None,
) -> User:
    user = get_user(db, user_id)
    changes = data.model_dump(exclude_unset=True)

    loses_admin = (
        user.role == UserRole.ADMIN
        and user.is_active
        and (
            (changes.get("role") not in (None, UserRole.ADMIN)) or changes.get("is_active") is False
        )
    )
    if loses_admin and _other_active_admins(db, user.id) == 0:
        raise ConflictError("Cannot demote or deactivate the last active admin")

    changed: dict[str, object] = {}
    for field in ("full_name", "role", "is_active"):
        new_value = changes.get(field)
        if new_value is not None and getattr(user, field) != new_value:
            changed[field] = {"from": getattr(user, field), "to": new_value}
            setattr(user, field, new_value)
    password_changed = bool(changes.get("password"))
    if password_changed:
        user.hashed_password = hash_password(changes["password"])

    deactivated = "is_active" in changed and user.is_active is False
    if deactivated or password_changed:
        revoke_all_refresh_tokens(db, user.id)

    if changed or password_changed:
        audit_service.record(
            db,
            action="user.deactivate" if deactivated else "user.update",
            entity_type="user",
            entity_id=user.id,
            actor=actor,
            details={"changes": changed, "password_changed": password_changed},
            ip_address=ip_address,
        )
    db.commit()
    return user
