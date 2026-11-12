import hashlib
import hmac
import json
from datetime import UTC, datetime
from typing import Any, Literal

import structlog
from fastapi import APIRouter, HTTPException, Request, Response, status
from slowapi.errors import RateLimitExceeded

from app.config import Settings, get_settings
from app.schemas.events import WebhookEnvelope
from app.services.idempotency import clear_seen_event, is_duplicate_event
from app.services.metrics import WEBHOOK_EVENTS_RECEIVED_TOTAL
from app.services.rate_limit import check_webhook_rate_limit, webhook_limit_decorator

logger = structlog.get_logger(__name__)

router = APIRouter(prefix="/webhooks", tags=["webhooks"])


def _get_app_settings(request: Request) -> Settings:
    return getattr(request.app.state, "settings", None) or get_settings()


def _verify_github_signature(raw_body: bytes, signature_header: str | None, secret: str) -> None:
    if not secret:
        logger.error("webhook_secret_not_configured", source="github")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="GitHub webhook secret is not configured",
        )
    if not signature_header or not signature_header.startswith("sha256="):
        WEBHOOK_EVENTS_RECEIVED_TOTAL.labels(
            source="github", event_type="unknown", status="unauthorized"
        ).inc()
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing or invalid GitHub webhook signature",
        )
    expected = (
        "sha256="
        + hmac.new(secret.encode("utf-8"), raw_body, hashlib.sha256).hexdigest()
    )
    if not hmac.compare_digest(expected, signature_header):
        WEBHOOK_EVENTS_RECEIVED_TOTAL.labels(
            source="github", event_type="unknown", status="unauthorized"
        ).inc()
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid GitHub webhook signature",
        )


def _verify_jira_token(token_header: str | None, secret: str) -> None:
    if not secret:
        logger.error("webhook_secret_not_configured", source="jira")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Jira webhook secret is not configured",
        )
    if not token_header or not hmac.compare_digest(secret, token_header):
        WEBHOOK_EVENTS_RECEIVED_TOTAL.labels(
            source="jira", event_type="unknown", status="unauthorized"
        ).inc()
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid Jira webhook secret token",
        )


def _enforce_rate_limit(
    request: Request,
    endpoint_func: Any,
    source: Literal["github", "jira"],
    secret: str,
) -> None:
    try:
        check_webhook_rate_limit(request, endpoint_func, source, secret)
    except RateLimitExceeded:
        WEBHOOK_EVENTS_RECEIVED_TOTAL.labels(
            source=source, event_type="unknown", status="rate_limited"
        ).inc()
        raise


def _parse_json_dict(raw_body: bytes) -> dict[str, Any]:
    try:
        parsed = json.loads(raw_body)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Request body must be valid JSON",
        ) from exc
    if not isinstance(parsed, dict):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Webhook payload must be a JSON object",
        )
    return parsed


async def _ingest_event(
    request: Request,
    source: Literal["github", "jira"],
    event_id: str,
    event_type: str,
    raw_body: bytes,
    payload: dict[str, Any],
) -> dict[str, Any]:
    received_at = datetime.now(UTC)
    redis_client = request.app.state.redis
    s3_archive = request.app.state.s3_archive
    kafka_producer = request.app.state.kafka_producer

    if await is_duplicate_event(redis_client, event_id):
        WEBHOOK_EVENTS_RECEIVED_TOTAL.labels(
            source=source, event_type=event_type, status="deduplicated"
        ).inc()
        logger.info("webhook_deduplicated", event_id=event_id, source=source, type=event_type)
        return {"status": "accepted", "event_id": event_id, "deduplicated": True}

    envelope = WebhookEnvelope(
        event_id=event_id,
        source=source,
        type=event_type,
        received_at=received_at,
        payload=payload,
    )

    try:
        s3_key = await s3_archive.archive_raw_event(source, event_id, received_at, raw_body)
        topic = await kafka_producer.publish_event(envelope)
    except Exception:
        # Release idempotency lock on transient failures so source retries can recover.
        WEBHOOK_EVENTS_RECEIVED_TOTAL.labels(
            source=source, event_type=event_type, status="failed"
        ).inc()
        await clear_seen_event(redis_client, event_id)
        raise

    WEBHOOK_EVENTS_RECEIVED_TOTAL.labels(
        source=source, event_type=event_type, status="accepted"
    ).inc()
    logger.info(
        "webhook_ingested",
        event_id=event_id,
        source=source,
        type=event_type,
        s3_key=s3_key,
        topic=topic,
    )
    return {"status": "accepted", "event_id": event_id, "deduplicated": False}


@router.post("/github", status_code=status.HTTP_202_ACCEPTED)
@webhook_limit_decorator()
async def receive_github_webhook(request: Request, response: Response) -> dict[str, Any]:
    raw_body = await request.body()
    settings = _get_app_settings(request)
    _verify_github_signature(
        raw_body,
        request.headers.get("X-Hub-Signature-256"),
        settings.GITHUB_WEBHOOK_SECRET,
    )
    _enforce_rate_limit(
        request,
        receive_github_webhook,
        "github",
        settings.GITHUB_WEBHOOK_SECRET,
    )

    payload = _parse_json_dict(raw_body)
    event_id = (
        request.headers.get("X-GitHub-Delivery")
        or str(payload.get("event_id") or "")
        or hashlib.sha256(raw_body).hexdigest()
    )
    event_type = request.headers.get("X-GitHub-Event") or str(payload.get("type", "unknown"))
    return await _ingest_event(request, "github", event_id, event_type, raw_body, payload)


@router.post("/jira", status_code=status.HTTP_202_ACCEPTED)
@webhook_limit_decorator()
async def receive_jira_webhook(request: Request, response: Response) -> dict[str, Any]:
    raw_body = await request.body()
    settings = _get_app_settings(request)
    _verify_jira_token(
        request.headers.get("X-Jira-Webhook-Secret"),
        settings.JIRA_WEBHOOK_SECRET,
    )
    _enforce_rate_limit(
        request,
        receive_jira_webhook,
        "jira",
        settings.JIRA_WEBHOOK_SECRET,
    )

    payload = _parse_json_dict(raw_body)
    event_id = (
        request.headers.get("X-Jira-Delivery")
        or str(payload.get("webhookEventId") or payload.get("event_id") or "")
        or hashlib.sha256(raw_body).hexdigest()
    )
    event_type = str(payload.get("webhookEvent") or payload.get("type") or "jira_event")
    return await _ingest_event(request, "jira", event_id, event_type, raw_body, payload)

