"""First-run bootstrap: create the initial admin when the users table is empty, and seed the
built-in automation workflows (disabled)."""

import logging

from sqlalchemy import func, select
from sqlalchemy.exc import OperationalError, ProgrammingError
from sqlalchemy.orm import Session, sessionmaker

from app.core.config import PLACEHOLDER, get_settings
from app.models.user import User, UserRole
from app.schemas.user import PASSWORD_MIN_LENGTH, UserCreate
from app.services import audit_service, automation_service, user_service

logger = logging.getLogger(__name__)


def bootstrap_admin(db: Session) -> User | None:
    if (db.scalar(select(func.count()).select_from(User)) or 0) > 0:
        return None

    settings = get_settings()
    if settings.ADMIN_PASSWORD == PLACEHOLDER or len(settings.ADMIN_PASSWORD) < PASSWORD_MIN_LENGTH:
        logger.warning(
            "No users exist and ADMIN_PASSWORD is unset or shorter than %d characters; "
            "skipping admin bootstrap. Set ADMIN_EMAIL/ADMIN_PASSWORD in backend/.env.",
            PASSWORD_MIN_LENGTH,
        )
        return None

    admin = user_service.create_user(
        db,
        UserCreate(
            email=settings.ADMIN_EMAIL,
            full_name="Administrator",
            password=settings.ADMIN_PASSWORD,
            role=UserRole.ADMIN,
        ),
        actor=None,
    )
    audit_service.record(
        db,
        action="system.bootstrap_admin",
        entity_type="user",
        entity_id=admin.id,
        details={"email": admin.email},
    )
    db.commit()
    logger.info("Created initial admin %s", admin.email)
    return admin


def run_bootstrap(session_factory: sessionmaker[Session]) -> None:
    try:
        with session_factory() as db:
            bootstrap_admin(db)
    except (OperationalError, ProgrammingError):
        logger.error(
            "Database is not initialised. Run `alembic upgrade head` from the backend directory."
        )
        return
    except ValueError as exc:  # includes pydantic.ValidationError (e.g. invalid ADMIN_EMAIL)
        logger.error("Admin bootstrap failed: %s", exc)
    try:
        with session_factory() as db:
            automation_service.seed_templates(db)  # built-in workflows, disabled
    except (OperationalError, ProgrammingError):
        logger.error("Automation tables are missing. Run `alembic upgrade head`.")
