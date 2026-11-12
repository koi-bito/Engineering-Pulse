from collections.abc import Awaitable, Callable
from typing import Any

from elasticsearch import NotFoundError
from fastapi import APIRouter, HTTPException, Query, Request, status

from app.config import Settings, get_settings
from app.db.pool import acquire_connection
from app.db.repositories import (
    fetch_active_sprint,
    fetch_cycle_time_series,
    fetch_metrics_summary,
    fetch_pr_daily_counts,
    search_events_postgres,
)
from app.services.cache import (
    AVG_CYCLE_TIME_7D_KEY,
    METRICS_CACHE_TTL_SECONDS,
    METRICS_SUMMARY_KEY,
    PRS_MERGED_TODAY_KEY,
    SEARCH_CACHE_TTL_SECONDS,
    SPRINT_CACHE_TTL_SECONDS,
    get_or_compute_cached,
    metrics_cycle_time_key,
    metrics_prs_key,
    search_cache_key,
    set_cached_json,
    sprint_active_key,
)
from app.services.es_index import ES_ALIAS

router = APIRouter(prefix="/api", tags=["metrics"])

_RANGE_DAYS = {"7d": 7, "14d": 14, "30d": 30, "90d": 90}


def _get_settings(request: Request) -> Settings:
    return getattr(request.app.state, "settings", None) or get_settings()


def _parse_range_days(range_str: str) -> int:
    days = _RANGE_DAYS.get(range_str)
    if days is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="range must be one of: 7d, 14d, 30d, 90d",
        )
    return days


async def _run_db_query(
    request: Request,
    query_fn: Callable[..., Awaitable[Any]],
    *args: Any,
) -> Any:
    async with acquire_connection(request.app.state.db_pool, "api") as conn:
        return await query_fn(conn, *args)


@router.get("/metrics/summary")
async def get_metrics_summary(request: Request) -> dict[str, Any]:
    redis_client = request.app.state.redis

    async def _load() -> dict[str, Any]:
        summary = await _run_db_query(request, fetch_metrics_summary)
        # Keep the documented individual metric keys populated alongside the aggregate summary.
        await set_cached_json(
            redis_client,
            PRS_MERGED_TODAY_KEY,
            summary["prs_merged_today"],
            METRICS_CACHE_TTL_SECONDS,
        )
        await set_cached_json(
            redis_client,
            AVG_CYCLE_TIME_7D_KEY,
            summary["avg_cycle_time_7d_hours"],
            METRICS_CACHE_TTL_SECONDS,
        )
        return summary

    summary, _ = await get_or_compute_cached(
        redis_client, METRICS_SUMMARY_KEY, METRICS_CACHE_TTL_SECONDS, _load
    )
    return summary


@router.get("/metrics/prs")
async def get_pr_metrics(
    request: Request,
    range: str = Query(default="7d"),
) -> dict[str, Any]:
    days = _parse_range_days(range)
    redis_client = request.app.state.redis

    async def _load() -> dict[str, Any]:
        series = await _run_db_query(request, fetch_pr_daily_counts, days)
        return {"range": range, "days": days, "series": series}

    data, _ = await get_or_compute_cached(
        redis_client, metrics_prs_key(range), METRICS_CACHE_TTL_SECONDS, _load
    )
    return data


@router.get("/metrics/cycle-time")
async def get_cycle_time_metrics(
    request: Request,
    range: str = Query(default="30d"),
) -> dict[str, Any]:
    days = _parse_range_days(range)
    redis_client = request.app.state.redis

    async def _load() -> dict[str, Any]:
        series = await _run_db_query(request, fetch_cycle_time_series, days)
        return {"range": range, "days": days, "series": series}

    data, _ = await get_or_compute_cached(
        redis_client, metrics_cycle_time_key(range), METRICS_CACHE_TTL_SECONDS, _load
    )
    return data


@router.get("/sprint/active")
async def get_active_sprint(
    request: Request,
    project: str | None = Query(default=None),
) -> dict[str, Any]:
    project_key = project.strip() if project and project.strip() else None
    cache_key = sprint_active_key(project_key or "all")
    redis_client = request.app.state.redis

    async def _load() -> dict[str, Any]:
        return await _run_db_query(request, fetch_active_sprint, project_key)

    data, _ = await get_or_compute_cached(
        redis_client, cache_key, SPRINT_CACHE_TTL_SECONDS, _load
    )
    return data


async def _search_elasticsearch(es_client: Any, query: str, limit: int) -> dict[str, Any]:
    try:
        response = await es_client.search(
            index=ES_ALIAS,
            query={
                "multi_match": {
                    "query": query,
                    "fields": ["title", "summary", "message"],
                }
            },
            size=limit,
        )
    except NotFoundError:
        return {"query": query, "total_hits": 0, "results": []}

    hits_block = response.get("hits", {})
    raw_hits = hits_block.get("hits", [])
    results = []
    for hit in raw_hits:
        src = hit.get("_source", {})
        results.append(
            {
                "id": hit.get("_id"),
                "type": src.get("type"),
                "source": src.get("source"),
                "author": src.get("author"),
                "repo": src.get("repo"),
                "timestamp": src.get("timestamp"),
                "snippet": src.get("title") or src.get("summary") or src.get("message") or "",
                "score": hit.get("_score"),
            }
        )
    total_meta = hits_block.get("total", len(results))
    total_hits = (
        int(total_meta.get("value", len(results)))
        if isinstance(total_meta, dict)
        else int(total_meta)
    )
    return {"query": query, "total_hits": total_hits, "results": results}


@router.get("/search")
async def search_events(
    request: Request,
    q: str = Query(..., min_length=1, max_length=200),
    limit: int = Query(default=20, ge=1, le=100),
) -> dict[str, Any]:
    cleaned = q.strip()
    if not cleaned:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Search query 'q' cannot be blank",
        )

    cfg = _get_settings(request)
    redis_client = request.app.state.redis
    es_client = getattr(request.app.state, "es_client", None)

    async def _load() -> dict[str, Any]:
        if cfg.ES_ENABLED and es_client is not None:
            return await _search_elasticsearch(es_client, cleaned, limit)
        results = await _run_db_query(request, search_events_postgres, cleaned, limit)
        return {"query": cleaned, "total_hits": len(results), "results": results}

    data, _ = await get_or_compute_cached(
        redis_client, search_cache_key(cleaned, limit), SEARCH_CACHE_TTL_SECONDS, _load
    )
    return data

