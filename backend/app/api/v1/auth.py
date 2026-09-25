from typing import Annotated

from fastapi import APIRouter, Cookie, Depends, Response, status

from app.core.config import get_settings
from app.core.dependencies import ClientIp, CurrentUser, DbSession, get_login_limiter
from app.core.rate_limit import LoginRateLimiter
from app.schemas.user import AccessTokenResponse, LoginRequest, UserRead
from app.services import auth_service
from app.services.auth_service import IssuedTokens

router = APIRouter(prefix="/auth", tags=["auth"])

REFRESH_COOKIE = "refresh_token"
RefreshCookie = Annotated[str | None, Cookie(alias=REFRESH_COOKIE)]


def _cookie_path() -> str:
    return f"{get_settings().API_V1_STR}/auth"


def _set_refresh_cookie(response: Response, tokens: IssuedTokens) -> None:
    settings = get_settings()
    response.set_cookie(
        REFRESH_COOKIE,
        tokens.refresh_token,
        max_age=settings.REFRESH_TOKEN_EXPIRE_DAYS * 24 * 3600,
        httponly=True,
        secure=not settings.is_development,
        samesite="strict",
        path=_cookie_path(),
    )


def _token_response(tokens: IssuedTokens) -> AccessTokenResponse:
    return AccessTokenResponse(
        access_token=tokens.access_token,
        expires_in=tokens.expires_in,
        user=UserRead.model_validate(tokens.user),
    )


@router.post("/login", response_model=AccessTokenResponse)
def login(
    body: LoginRequest,
    response: Response,
    db: DbSession,
    ip: ClientIp,
    limiter: Annotated[LoginRateLimiter, Depends(get_login_limiter)],
) -> AccessTokenResponse:
    tokens = auth_service.login(
        db, email=body.email, password=body.password, limiter=limiter, ip_address=ip
    )
    _set_refresh_cookie(response, tokens)
    return _token_response(tokens)


@router.post("/refresh", response_model=AccessTokenResponse)
def refresh(
    response: Response, db: DbSession, ip: ClientIp, refresh_token: RefreshCookie = None
) -> AccessTokenResponse:
    tokens = auth_service.refresh(db, refresh_token, ip_address=ip)
    _set_refresh_cookie(response, tokens)
    return _token_response(tokens)


@router.post("/logout", status_code=status.HTTP_204_NO_CONTENT)
def logout(
    response: Response, db: DbSession, ip: ClientIp, refresh_token: RefreshCookie = None
) -> Response:
    auth_service.logout(db, refresh_token, ip_address=ip)
    response.status_code = status.HTTP_204_NO_CONTENT
    response.delete_cookie(REFRESH_COOKIE, path=_cookie_path())
    return response


@router.get("/me", response_model=UserRead)
def me(user: CurrentUser) -> UserRead:
    return UserRead.model_validate(user)
