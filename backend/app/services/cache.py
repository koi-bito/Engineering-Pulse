import hashlib
import json
from collections.abc import Awaitable, Callable
from typing import Any

from app.services.metrics import record_cache_lookup

PRS_MERGED_TODAY_KEY = "metrics:prs_merged_today"
AVG_CYCLE_TIME_7D_KEY = "metrics:avg_cycle_time_7d"
METRICS_SUMMARY_KEY = "metrics:summary"
METRICS_CACHE_TTL_SECONDS = 300
SPRINT_CACHE_TTL_SECONDS = 60
SEARCH_CACHE_TTL_SECONDS = 60
SUPPORTED_METRIC_RANGES = ("7d", "14d", "30d", "90d")


def tenant_prefix(org_id: Any | None = None, team_id: Any | None = None) -> str:
    if org_id and team_id:
        return f"{org_id}:{team_id}:"
    if team_id:
        return f"team:{team_id}:"
    return ""


def metrics_summary_key(org_id: Any | None = None, team_id: Any | None = None) -> str:
    return f"{tenant_prefix(org_id, team_id)}metrics:summary"


def sprint_active_key(
    project_key: str,
    org_id: Any | None = None,
    team_id: Any | None = None,
) -> str:
    return f"{tenant_prefix(org_id, team_id)}sprint:active:{project_key}"


def metrics_prs_key(
    range_str: str,
    org_id: Any | None = None,
    team_id: Any | None = None,
) -> str:
    return f"{tenant_prefix(org_id, team_id)}metrics:prs:{range_str}"


def metrics_cycle_time_key(
    range_str: str,
    org_id: Any | None = None,
    team_id: Any | None = None,
) -> str:
    return f"{tenant_prefix(org_id, team_id)}metrics:cycle_time:{range_str}"


def search_cache_key(
    query: str,
    limit: int = 20,
    org_id: Any | None = None,
    team_id: Any | None = None,
) -> str:
    digest = hashlib.sha256(query.strip().lower().encode("utf-8")).hexdigest()[:16]
    return f"{tenant_prefix(org_id, team_id)}search:{digest}:{limit}"


async def get_cached_json(redis_client: Any, key: str) -> Any | None:
    raw = await redis_client.get(key)
    if raw is None:
        return None
    return json.loads(raw)


async def set_cached_json(
    redis_client: Any,
    key: str,
    value: Any,
    ttl_seconds: int,
) -> None:
    await redis_client.set(key, json.dumps(value), ex=ttl_seconds)


async def get_or_compute_cached(
    redis_client: Any,
    key: str,
    ttl_seconds: int,
    loader: Callable[[], Awaitable[Any]],
) -> tuple[Any, bool]:
    """Read-through cache lookup returning (value, cache_hit) and updating hit ratio."""
    cached = await get_cached_json(redis_client, key)
    if cached is not None:
        record_cache_lookup(True)
        return cached, True

    record_cache_lookup(False)
    value = await loader()
    await set_cached_json(redis_client, key, value, ttl_seconds)
    return value, False


async def invalidate_metrics_cache(
    redis_client: Any,
    project_key: str | None = None,
    org_id: Any | None = None,
    team_id: Any | None = None,
) -> None:
    keys = [
        PRS_MERGED_TODAY_KEY,
        AVG_CYCLE_TIME_7D_KEY,
        METRICS_SUMMARY_KEY,
        sprint_active_key("all"),
        *(metrics_prs_key(r) for r in SUPPORTED_METRIC_RANGES),
        *(metrics_cycle_time_key(r) for r in SUPPORTED_METRIC_RANGES),
    ]
    if project_key:
        keys.append(sprint_active_key(project_key))

    if org_id or team_id:
        pfx = tenant_prefix(org_id, team_id)
        keys.extend(
            [
                f"{pfx}{PRS_MERGED_TODAY_KEY}",
                f"{pfx}{AVG_CYCLE_TIME_7D_KEY}",
                metrics_summary_key(org_id, team_id),
                sprint_active_key("all", org_id, team_id),
                *(metrics_prs_key(r, org_id, team_id) for r in SUPPORTED_METRIC_RANGES),
                *(metrics_cycle_time_key(r, org_id, team_id) for r in SUPPORTED_METRIC_RANGES),
            ]
        )
        if project_key:
            keys.append(sprint_active_key(project_key, org_id, team_id))

    await redis_client.delete(*keys)
