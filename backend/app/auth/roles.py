from collections.abc import Callable, Coroutine
from typing import Annotated, Any

from fastapi import Depends, HTTPException, status

from app.auth.backend import current_active_user
from app.models.user import User


def require_role(
    allowed_roles: str | list[str],
) -> Callable[[User], Coroutine[Any, Any, User]]:
    """Dependency that ensures the authenticated user possesses an allowed role."""
    roles = [allowed_roles] if isinstance(allowed_roles, str) else allowed_roles

    async def role_checker(
        user: Annotated[User, Depends(current_active_user)],
    ) -> User:
        if user.is_superuser or user.role in roles:
            return user
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Forbidden: requires role in {roles}",
        )

    return role_checker
