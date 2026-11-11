import json
from typing import Any

PRS_MERGED_TODAY_KEY = "metrics:prs_merged_today"
AVG_CYCLE_TIME_7D_KEY = "metrics:avg_cycle_time_7d"
METRICS_CACHE_TTL_SECONDS = 300
SPRINT_CACHE_TTL_SECONDS = 60


def sprint_active_key(project_key: str) -> str:
    return f"sprint:active:{project_key}"


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


async def invalidate_metrics_cache(
    redis_client: Any,
    project_key: str | None = None,
) -> None:
    keys = [PRS_MERGED_TODAY_KEY, AVG_CYCLE_TIME_7D_KEY]
    if project_key:
        keys.append(sprint_active_key(project_key))
    await redis_client.delete(*keys)

