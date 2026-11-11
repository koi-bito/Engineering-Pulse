import hashlib
from collections.abc import Callable
from typing import Any

from fastapi import Request
from limits.storage import storage_from_string
from limits.strategies import STRATEGIES
from slowapi import Limiter
from slowapi.util import get_remote_address

from app.config import get_settings

_active_rate_limit: str | None = None


def hash_secret_key(source: str, secret: str) -> str:
    digest = hashlib.sha256(secret.encode("utf-8")).hexdigest()[:16]
    return f"webhook:{source}:{digest}"


def webhook_rate_limit_key(request: Request) -> str:
    # Set after signature verification so unauthenticated requests cannot drain a secret's quota.
    key = getattr(request.state, "rate_limit_key", None)
    return key if isinstance(key, str) and key else f"ip:{get_remote_address(request)}"


def _webhook_rate_limit_value() -> str:
    return _active_rate_limit or get_settings().WEBHOOK_RATE_LIMIT


webhook_limiter = Limiter(
    key_func=webhook_rate_limit_key,
    headers_enabled=True,
    storage_uri=get_settings().REDIS_URL,
    in_memory_fallback_enabled=True,
    auto_check=False,
)


def configure_webhook_limiter(
    limiter: Limiter,
    storage_uri: str,
    rate_limit: str | None = None,
) -> None:
    """Configure limiter storage and active rate limit for app startup or tests."""
    global _active_rate_limit
    _active_rate_limit = rate_limit
    limiter._storage = storage_from_string(storage_uri)
    limiter._limiter = STRATEGIES["fixed-window"](limiter._storage)
    limiter._storage_dead = False


def check_webhook_rate_limit(
    request: Request,
    endpoint_func: Callable[..., Any],
    source: str,
    secret: str,
) -> None:
    request.state.rate_limit_key = hash_secret_key(source, secret)
    limiter: Limiter = request.app.state.limiter
    limiter._check_request_limit(request, endpoint_func, False)
    request.state._rate_limiting_complete = True


def webhook_limit_decorator():
    return webhook_limiter.limit(_webhook_rate_limit_value)
