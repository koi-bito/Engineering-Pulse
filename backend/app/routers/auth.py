import uuid
from typing import Annotated, Any

import structlog
from fastapi import APIRouter, Depends, HTTPException, Request, Response, status
from fastapi.security import OAuth2PasswordRequestForm
from fastapi_users import exceptions
from slowapi import Limiter
from slowapi.util import get_remote_address

from app.auth.backend import (
    REFRESH_COOKIE_NAME,
    REFRESH_TOKEN_AUDIENCE,
    current_active_user,
    decode_auth_token,
    generate_access_token,
    generate_refresh_token,
)
from app.auth.manager import UserManager, get_user_manager
from app.auth.roles import require_role
from app.auth.schemas import RegisterRequest, TokenResponse, UserCreate, UserRead
from app.config import get_settings
from app.models.user import User

logger = structlog.get_logger(__name__)
_settings = get_settings()

auth_limiter = Limiter(
    key_func=get_remote_address,
    headers_enabled=True,
    default_limits=[],
    auto_check=False,
)


def configure_auth_limiter(storage_uri: str) -> None:
    from limits.storage import storage_from_string
    from limits.strategies import STRATEGIES

    auth_limiter._storage = storage_from_string(storage_uri)
    auth_limiter._limiter = STRATEGIES["fixed-window"](auth_limiter._storage)
    auth_limiter._storage_dead = False


def check_auth_rate_limit(request: Request, endpoint_func: Any) -> None:
    limiter = getattr(request.app.state, "auth_limiter", auth_limiter)
    limiter._check_request_limit(request, endpoint_func, False)
    request.state._rate_limiting_complete = True


router = APIRouter(tags=["auth"])


@router.post(
    "/auth/jwt/login",
    response_model=TokenResponse,
    summary="Login with email and password",
)
@auth_limiter.limit(_settings.AUTH_LOGIN_RATE_LIMIT)
async def login(
    request: Request,
    response: Response,
    credentials: Annotated[OAuth2PasswordRequestForm, Depends()],
    user_manager: Annotated[UserManager, Depends(get_user_manager)],
) -> TokenResponse:
    """Authenticate user with email and password, returning an access token and setting a secure httpOnly refresh cookie."""
    check_auth_rate_limit(request, login)
    user = await user_manager.authenticate(credentials)
    if user is None or not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="LOGIN_BAD_CREDENTIALS",
        )

    access_token = generate_access_token(user)
    refresh_token = generate_refresh_token(user)

    response.set_cookie(
        key=REFRESH_COOKIE_NAME,
        value=refresh_token,
        max_age=_settings.REFRESH_TOKEN_LIFETIME_SECONDS,
        httponly=True,
        secure=(_settings.ENVIRONMENT == "production"),
        samesite="lax",
        path="/",
    )

    await user_manager.on_after_login(user, request=request)
    return TokenResponse(
        access_token=access_token,
        token_type="bearer",
        user=UserRead.model_validate(user),
    )


@router.post(
    "/auth/jwt/refresh",
    response_model=TokenResponse,
    summary="Refresh access token using httpOnly refresh cookie",
)
@auth_limiter.limit(_settings.AUTH_REFRESH_RATE_LIMIT)
async def refresh_token_endpoint(
    request: Request,
    response: Response,
    user_manager: Annotated[UserManager, Depends(get_user_manager)],
) -> TokenResponse:
    """Exchange a valid httpOnly refresh cookie for a fresh access token and rotated refresh cookie."""
    check_auth_rate_limit(request, refresh_token_endpoint)
    cookie_token = request.cookies.get(REFRESH_COOKIE_NAME)
    if not cookie_token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing refresh token cookie",
        )

    try:
        payload = decode_auth_token(cookie_token, audience=REFRESH_TOKEN_AUDIENCE)
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired refresh token",
        ) from exc

    user_id_str = payload.get("sub")
    if not user_id_str:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid refresh token payload",
        )

    try:
        user_uuid = uuid.UUID(user_id_str)
        user = await user_manager.get(user_uuid)
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User not found",
        ) from exc

    if not user or not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Inactive user",
        )

    new_access_token = generate_access_token(user)
    new_refresh_token = generate_refresh_token(user)

    response.set_cookie(
        key=REFRESH_COOKIE_NAME,
        value=new_refresh_token,
        max_age=_settings.REFRESH_TOKEN_LIFETIME_SECONDS,
        httponly=True,
        secure=(_settings.ENVIRONMENT == "production"),
        samesite="lax",
        path="/",
    )

    return TokenResponse(
        access_token=new_access_token,
        token_type="bearer",
        user=UserRead.model_validate(user),
    )


@router.post(
    "/auth/jwt/logout",
    status_code=status.HTTP_200_OK,
    summary="Logout user and clear refresh cookie",
)
async def logout(response: Response) -> dict[str, str]:
    """Clear the httpOnly refresh cookie."""
    response.delete_cookie(
        key=REFRESH_COOKIE_NAME,
        path="/",
        httponly=True,
        secure=(_settings.ENVIRONMENT == "production"),
        samesite="lax",
    )
    return {"status": "logged_out"}


@router.post(
    "/api/auth/register",
    response_model=UserRead,
    status_code=status.HTTP_201_CREATED,
    summary="Register a new organization, team, and first admin user",
)
@auth_limiter.limit(_settings.AUTH_REGISTER_RATE_LIMIT)
async def register(
    request: Request,
    response: Response,
    body: RegisterRequest,
    user_manager: Annotated[UserManager, Depends(get_user_manager)],
) -> UserRead:
    """Create an organization, initial team, and first admin user in one transaction."""
    check_auth_rate_limit(request, register)
    org_id = uuid.uuid4()
    team_id = uuid.uuid4()

    user_create = UserCreate(
        email=body.email,
        password=body.password,
        org_id=org_id,
        team_id=team_id,
        role="admin",
    )

    try:
        created_user = await user_manager.create(user_create, request=request)
    except exceptions.UserAlreadyExists:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="REGISTER_USER_ALREADY_EXISTS",
        )
    logger.info(
        "org_and_admin_created",
        org_name=body.org_name,
        team_name=body.team_name,
        user_id=str(created_user.id),
        email=created_user.email,
    )
    return UserRead.model_validate(created_user)


@router.get(
    "/api/auth/me",
    response_model=UserRead,
    summary="Get current authenticated user profile",
)
async def get_me(user: Annotated[User, Depends(current_active_user)]) -> UserRead:
    """Return profile of current authenticated user."""
    return UserRead.model_validate(user)


@router.get(
    "/api/auth/admin-check",
    summary="Check admin permission",
)
async def admin_check(
    admin_user: Annotated[User, Depends(require_role("admin"))],
) -> dict[str, str]:
    """Protected endpoint verifying admin role."""
    return {"status": "authorized", "role": admin_user.role}
