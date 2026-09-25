import uuid

from fastapi import APIRouter, status

from app.core.dependencies import AdminUser, ClientIp, DbSession, PageParams
from app.models.user import UserRole
from app.schemas.common import Page
from app.schemas.user import UserCreate, UserRead, UserUpdate
from app.services import user_service

router = APIRouter(prefix="/users", tags=["users"])


@router.get("", response_model=Page[UserRead])
def list_users(
    db: DbSession, _: AdminUser, page: PageParams, role: UserRole | None = None
) -> Page[UserRead]:
    items, total = user_service.list_users(db, role=role, limit=page.limit, offset=page.offset)
    return Page[UserRead](
        items=[UserRead.model_validate(u) for u in items],
        total=total,
        limit=page.limit,
        offset=page.offset,
    )


@router.post("", response_model=UserRead, status_code=status.HTTP_201_CREATED)
def create_user(body: UserCreate, db: DbSession, admin: AdminUser, ip: ClientIp) -> UserRead:
    return UserRead.model_validate(user_service.create_user(db, body, actor=admin, ip_address=ip))


@router.patch("/{user_id}", response_model=UserRead)
def update_user(
    user_id: uuid.UUID, body: UserUpdate, db: DbSession, admin: AdminUser, ip: ClientIp
) -> UserRead:
    user = user_service.update_user(db, user_id, body, actor=admin, ip_address=ip)
    return UserRead.model_validate(user)
