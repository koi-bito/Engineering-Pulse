import uuid
from datetime import UTC, datetime, timedelta
from typing import Annotated, Any

import structlog
from fastapi import APIRouter, Depends, HTTPException, Request, Response, status
from pydantic import BaseModel, Field

from app.auth.backend import current_active_user
from app.auth.roles import require_role
from app.db.pool import acquire_connection
from app.db.repositories import (
    fetch_active_workflow_alerts,
    fetch_latest_snapshot_timestamp,
    get_or_create_alert_config,
    to_utc_iso,
    upsert_alert_config,
)
from app.models.user import User
from app.services.anomaly_detector import run_snapshot_and_anomaly_cycle

logger = structlog.get_logger(__name__)

router = APIRouter(prefix="/api", tags=["alerts"])


class AlertConfigUpdateRequest(BaseModel):
    metric_name: str = Field(min_length=2, max_length=100)
    std_multiplier: float = Field(default=2.0, ge=0.5, le=10.0)
    min_data_points: int = Field(default=7, ge=2, le=1000)
    suppression_window_minutes: int = Field(default=60, ge=1, le=1440)


class AlertResponse(BaseModel):
    id: uuid.UUID
    team_id: uuid.UUID
    metric_name: str
    actual_value: float
    expected_value: float
    severity: str
    status: str
    group_id: uuid.UUID | None = None
    details: dict[str, Any] = Field(default_factory=dict)
    created_at: str | None = None
    resolved_at: str | None = None


@router.get("/health/snapshots", summary="Check metrics snapshots freshness")
async def check_snapshots_health(request: Request, response: Response) -> dict[str, Any]:
    """Return health and age of the latest recorded metrics snapshot."""
    db_pool = getattr(request.app.state, "db_pool", None)
    if not db_pool:
        return {"status": "no_data", "detail": "Database pool not initialized"}

    async with acquire_connection(db_pool, "api") as conn:
        latest = await fetch_latest_snapshot_timestamp(conn)

    if latest is None:
        return {"status": "no_data", "last_snapshot": None}

    now = datetime.now(UTC)
    latest_utc = latest.replace(tzinfo=UTC) if latest.tzinfo is None else latest.astimezone(UTC)
    age = now - latest_utc

    if age > timedelta(hours=2):
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        return {
            "status": "stale",
            "last_snapshot": to_utc_iso(latest),
            "age_seconds": round(age.total_seconds()),
        }

    return {
        "status": "ok",
        "last_snapshot": to_utc_iso(latest),
        "age_seconds": round(age.total_seconds()),
    }


@router.get(
    "/alerts/active",
    response_model=list[AlertResponse],
    summary="Get active and grouped workflow alerts for current team",
)
async def get_active_alerts(
    request: Request,
    user: Annotated[User, Depends(current_active_user)],
) -> list[dict[str, Any]]:
    """List active anomalies and grouped alerts scoped to the user's active team."""
    team_id = getattr(request.state, "team_id", None) or user.team_id
    if not team_id:
        return []

    async with acquire_connection(request.app.state.db_pool, "api") as conn:
        return await fetch_active_workflow_alerts(conn, team_id=team_id)


@router.get(
    "/alerts/config",
    summary="Get alert configuration for a specific metric",
)
async def get_alert_config(
    request: Request,
    metric_name: str,
    user: Annotated[User, Depends(current_active_user)],
) -> dict[str, Any]:
    """Get threshold and suppression settings for a metric."""
    team_id = getattr(request.state, "team_id", None) or user.team_id
    async with acquire_connection(request.app.state.db_pool, "api") as conn:
        return await get_or_create_alert_config(conn, team_id, metric_name)


@router.post(
    "/alerts/config",
    summary="Update alert configuration for a team metric",
)
async def update_alert_config(
    request: Request,
    body: AlertConfigUpdateRequest,
    admin_user: Annotated[User, Depends(require_role(["admin"]))],
) -> dict[str, Any]:
    """Admin updates threshold and suppression settings for a metric without deploying code."""
    team_id = getattr(request.state, "team_id", None) or admin_user.team_id
    if not team_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Admin has no active team context",
        )

    async with acquire_connection(request.app.state.db_pool, "api") as conn, conn.transaction():
        updated = await upsert_alert_config(
            conn,
            team_id=team_id,
            metric_name=body.metric_name,
            std_multiplier=body.std_multiplier,
            min_data_points=body.min_data_points,
            suppression_window_minutes=body.suppression_window_minutes,
        )
    logger.info(
        "alert_config_updated",
        team_id=str(team_id),
        metric=body.metric_name,
        std_multiplier=body.std_multiplier,
    )
    return updated


@router.post(
    "/alerts/run-cycle",
    summary="Manually trigger snapshot generation and anomaly detection",
)
async def trigger_anomaly_cycle(
    request: Request,
    admin_user: Annotated[User, Depends(require_role(["admin"]))],
) -> dict[str, Any]:
    """Execute an immediate snapshot & anomaly evaluation cycle for all teams."""
    db_pool = request.app.state.db_pool
    redis_client = getattr(request.app.state, "redis", None)
    return await run_snapshot_and_anomaly_cycle(db_pool, redis_client=redis_client)
