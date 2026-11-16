import uuid
from typing import Any

from fastapi_users import FastAPIUsers
from fastapi_users.authentication import AuthenticationBackend, BearerTransport, JWTStrategy
from fastapi_users.jwt import decode_jwt, generate_jwt

from app.auth.manager import get_user_manager
from app.config import get_settings
from app.models.user import User

_settings = get_settings()

bearer_transport = BearerTransport(tokenUrl="/auth/jwt/login")

ACCESS_TOKEN_AUDIENCE = ["fastapi-users:auth"]
REFRESH_TOKEN_AUDIENCE = ["fastapi-users:refresh"]
REFRESH_COOKIE_NAME = "ep_refresh_token"


class CustomJWTStrategy(JWTStrategy[User, uuid.UUID]):
    async def write_token(self, user: User) -> str:
        data: dict[str, Any] = {
            "sub": str(user.id),
            "aud": self.token_audience,
            "email": user.email,
            "role": user.role,
            "team_id": str(user.team_id) if user.team_id else None,
            "org_id": str(user.org_id) if user.org_id else None,
        }
        return generate_jwt(
            data,
            self.encode_key,
            self.lifetime_seconds,
            algorithm=self.algorithm,
        )


def get_jwt_strategy() -> CustomJWTStrategy:
    return CustomJWTStrategy(
        secret=_settings.JWT_SECRET,
        lifetime_seconds=_settings.JWT_LIFETIME_SECONDS,
        token_audience=ACCESS_TOKEN_AUDIENCE,
    )


auth_backend = AuthenticationBackend(
    name="jwt",
    transport=bearer_transport,
    get_strategy=get_jwt_strategy,
)

fastapi_users_app = FastAPIUsers[User, uuid.UUID](
    get_user_manager,
    [auth_backend],
)

current_active_user = fastapi_users_app.current_user(active=True)


def generate_access_token(user: User) -> str:
    data: dict[str, Any] = {
        "sub": str(user.id),
        "aud": ACCESS_TOKEN_AUDIENCE,
        "email": user.email,
        "role": user.role,
        "team_id": str(user.team_id) if user.team_id else None,
        "org_id": str(user.org_id) if user.org_id else None,
    }
    return generate_jwt(
        data,
        _settings.JWT_SECRET,
        lifetime_seconds=_settings.JWT_LIFETIME_SECONDS,
    )


def generate_refresh_token(user: User) -> str:
    data: dict[str, Any] = {
        "sub": str(user.id),
        "aud": REFRESH_TOKEN_AUDIENCE,
        "type": "refresh",
    }
    return generate_jwt(
        data,
        _settings.JWT_SECRET,
        lifetime_seconds=_settings.REFRESH_TOKEN_LIFETIME_SECONDS,
    )


def decode_auth_token(token: str, audience: list[str] | None = None) -> dict[str, Any]:
    return decode_jwt(
        token,
        _settings.JWT_SECRET,
        audience=audience or ACCESS_TOKEN_AUDIENCE,
        algorithms=["HS256"],
    )

