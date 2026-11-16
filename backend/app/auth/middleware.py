from collections.abc import Callable
from typing import Any

import structlog
from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware

from app.auth.backend import decode_auth_token

logger = structlog.get_logger(__name__)


class JWTAuthMiddleware(BaseHTTPMiddleware):
    """Custom middleware extracting JWT claims from Authorization Bearer token into request.state.

    All downstream endpoints read team_id and user claims from request.state, never from query parameters.
    """

    async def dispatch(
        self, request: Request, call_next: Callable[[Request], Any]
    ) -> Response:
        auth_header = request.headers.get("Authorization")
        request.state.user_id = None
        request.state.team_id = None
        request.state.org_id = None
        request.state.role = None

        if auth_header and auth_header.startswith("Bearer "):
            token = auth_header[7:].strip()
            try:
                payload = decode_auth_token(token)
                request.state.user_id = payload.get("sub")
                request.state.team_id = payload.get("team_id")
                request.state.org_id = payload.get("org_id")
                request.state.role = payload.get("role")
            except Exception as exc:  # noqa: BLE001
                # Malformed/expired token - log debug; protected dependencies will reject with 401
                logger.debug("jwt_middleware_token_decode_failed", error=str(exc))

        return await call_next(request)
