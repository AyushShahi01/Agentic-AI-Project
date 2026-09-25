import uuid
from collections.abc import Callable, Iterator
from typing import Annotated

import jwt
from fastapi import Depends, Query, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from sqlalchemy.orm import Session

from app.core.exceptions import AuthenticationError, PermissionDeniedError
from app.core.rate_limit import LoginRateLimiter
from app.core.security import decode_token
from app.models.user import User, UserRole


def get_db(request: Request) -> Iterator[Session]:
    db: Session = request.app.state.session_factory()
    try:
        yield db
    finally:
        db.close()


DbSession = Annotated[Session, Depends(get_db)]

_bearer = HTTPBearer(auto_error=False)


def get_current_user(
    db: DbSession,
    credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(_bearer)],
) -> User:
    if credentials is None:
        raise AuthenticationError("Not authenticated")
    try:
        payload = decode_token(credentials.credentials, "access")
        user_id = uuid.UUID(payload["sub"])
    except (jwt.InvalidTokenError, ValueError) as exc:
        raise AuthenticationError("Invalid or expired access token") from exc
    # Always load from the DB so deactivation and role changes apply immediately.
    user = db.get(User, user_id)
    if user is None or not user.is_active:
        raise AuthenticationError("User not found or inactive")
    return user


CurrentUser = Annotated[User, Depends(get_current_user)]


def require_role(*roles: UserRole) -> Callable[[User], User]:
    def _dependency(user: CurrentUser) -> User:
        if user.role not in roles:
            raise PermissionDeniedError(
                "You do not have permission to perform this action",
                details={"required_roles": list(roles)},
            )
        return user

    return _dependency


AdminUser = Annotated[User, Depends(require_role(UserRole.ADMIN))]
OperatorUser = Annotated[User, Depends(require_role(UserRole.ADMIN, UserRole.OPERATOR))]
ViewerUser = Annotated[
    User, Depends(require_role(UserRole.ADMIN, UserRole.OPERATOR, UserRole.VIEWER))
]


def get_client_ip(request: Request) -> str | None:
    return request.client.host if request.client else None


ClientIp = Annotated[str | None, Depends(get_client_ip)]


def get_login_limiter(request: Request) -> LoginRateLimiter:
    return request.app.state.login_limiter


class Pagination:
    def __init__(
        self,
        limit: Annotated[int, Query(ge=1, le=200)] = 50,
        offset: Annotated[int, Query(ge=0)] = 0,
    ) -> None:
        self.limit = limit
        self.offset = offset


PageParams = Annotated[Pagination, Depends()]
