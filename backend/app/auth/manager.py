import uuid
from collections.abc import AsyncGenerator
from typing import Annotated

import structlog
from fastapi import Depends, Request
from fastapi_users import BaseUserManager, UUIDIDMixin
from fastapi_users_db_sqlalchemy import SQLAlchemyUserDatabase

from app.auth.db import get_user_db
from app.config import get_settings
from app.models.user import User

logger = structlog.get_logger(__name__)
_settings = get_settings()


class UserManager(UUIDIDMixin, BaseUserManager[User, uuid.UUID]):
    reset_password_token_secret = _settings.JWT_SECRET
    verification_token_secret = _settings.JWT_SECRET

    async def on_after_register(self, user: User, request: Request | None = None) -> None:
        logger.info("user_registered", user_id=str(user.id), email=user.email, role=user.role)

    async def on_after_login(
        self,
        user: User,
        request: Request | None = None,
        response: Request | None = None,
    ) -> None:
        logger.info("user_logged_in", user_id=str(user.id), email=user.email)


async def get_user_manager(
    user_db: Annotated[SQLAlchemyUserDatabase[User, uuid.UUID], Depends(get_user_db)],
) -> AsyncGenerator[UserManager, None]:
    yield UserManager(user_db)
