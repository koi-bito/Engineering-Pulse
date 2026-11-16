import uuid
from datetime import datetime
from typing import Annotated, Any, Literal

import structlog
from fastapi import APIRouter, Depends, HTTPException, Request, status
from pydantic import BaseModel, Field

from app.auth.backend import current_active_user
from app.auth.roles import require_role
from app.db.pool import acquire_connection
from app.db.repositories import (
    create_team,
    fetch_teams_for_org,
    register_webhook,
)
from app.models.user import User

logger = structlog.get_logger(__name__)

router = APIRouter(prefix="/api", tags=["tenants"])


class WebhookRegisterRequest(BaseModel):
    team_id: uuid.UUID
    source: Literal["github", "jira"]
    secret: str = Field(min_length=8, max_length=255)


class WebhookRegisterResponse(BaseModel):
    id: uuid.UUID
    org_id: uuid.UUID
    team_id: uuid.UUID
    source: str
    created_at: datetime


class TeamCreateRequest(BaseModel):
    name: str = Field(min_length=2, max_length=100)


class TeamResponse(BaseModel):
    id: uuid.UUID
    org_id: uuid.UUID
    name: str
    created_at: datetime


@router.get("/teams", response_model=list[TeamResponse])
async def list_teams(
    request: Request,
    user: Annotated[User, Depends(current_active_user)],
) -> list[dict[str, Any]]:
    """List all teams belonging to the user's current organization."""
    org_id = user.org_id
    if not org_id:
        return []

    async with acquire_connection(request.app.state.db_pool, "api") as conn:
        return await fetch_teams_for_org(conn, org_id)


@router.post("/teams", response_model=TeamResponse, status_code=status.HTTP_201_CREATED)
async def create_new_team(
    request: Request,
    body: TeamCreateRequest,
    user: Annotated[User, Depends(require_role(["admin"]))],
) -> dict[str, Any]:
    """Create a new team within the admin's organization."""
    if not user.org_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="User does not belong to an organization",
        )

    async with acquire_connection(request.app.state.db_pool, "api") as conn, conn.transaction():
        return await create_team(conn, user.org_id, body.name)


@router.post(
    "/orgs/{org_id}/webhooks/register",
    response_model=WebhookRegisterResponse,
    status_code=status.HTTP_201_CREATED,
)
async def register_team_webhook(
    request: Request,
    org_id: uuid.UUID,
    body: WebhookRegisterRequest,
    user: Annotated[User, Depends(require_role(["admin"]))],
) -> dict[str, Any]:
    """Register a webhook secret for a team to map incoming webhooks to the correct tenant."""
    if user.org_id != org_id and not user.is_superuser:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Cannot register webhook for a different organization",
        )

    async with acquire_connection(request.app.state.db_pool, "api") as conn, conn.transaction():
        registration = await register_webhook(
            conn,
            org_id=org_id,
            team_id=body.team_id,
            source=body.source,
            secret=body.secret,
        )

    logger.info(
        "webhook_registered",
        org_id=str(org_id),
        team_id=str(body.team_id),
        source=body.source,
    )
    return registration
