import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime

import redis.asyncio as redis_async
import structlog
from botocore.exceptions import BotoCoreError, ClientError
from fastapi import FastAPI, HTTPException, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from slowapi.errors import RateLimitExceeded

from app.config import Settings, get_settings
from app.db.pool import create_pg_pool
from app.routers.webhooks import router as webhooks_router
from app.services.kafka_producer import KafkaEventProducer
from app.services.rate_limit import configure_webhook_limiter, webhook_limiter
from app.services.s3_archive import S3ArchiveService

logger = structlog.get_logger(__name__)

_HTTP_ERROR_CODES = {
    400: "bad_request",
    401: "unauthorized",
    404: "not_found",
    422: "validation_error",
    429: "rate_limit_exceeded",
    500: "internal_server_error",
}


def _configure_logging(environment: str) -> None:
    renderer = (
        structlog.dev.ConsoleRenderer()
        if environment == "local"
        else structlog.processors.JSONRenderer()
    )
    structlog.configure(
        processors=[
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.add_log_level,
            renderer,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            logging.DEBUG if environment == "local" else logging.INFO
        ),
    )


def _error_payload(error: str, detail: object, retry_after: int | None = None) -> dict[str, object]:
    body: dict[str, object] = {"error": error, "detail": detail}
    if retry_after is not None:
        body["retry_after"] = retry_after
    return body


def _register_exception_handlers(app: FastAPI) -> None:
    @app.exception_handler(HTTPException)
    async def http_exception_handler(_: Request, exc: HTTPException) -> JSONResponse:
        code = _HTTP_ERROR_CODES.get(exc.status_code, "http_error")
        return JSONResponse(
            status_code=exc.status_code,
            content=_error_payload(code, exc.detail),
            headers=exc.headers,
        )

    @app.exception_handler(RequestValidationError)
    async def validation_exception_handler(
        _: Request, exc: RequestValidationError
    ) -> JSONResponse:
        return JSONResponse(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            content=_error_payload("validation_error", exc.errors()),
        )

    @app.exception_handler(RateLimitExceeded)
    async def rate_limit_handler(request: Request, exc: RateLimitExceeded) -> JSONResponse:
        response = JSONResponse(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            content=_error_payload("rate_limit_exceeded", f"Rate limit exceeded: {exc.detail}"),
        )
        limiter = getattr(request.app.state, "limiter", None)
        view_rate_limit = getattr(request.state, "view_rate_limit", None)
        if limiter is not None and view_rate_limit is not None:
            response = limiter._inject_headers(response, view_rate_limit)
            retry_after_header = response.headers.get("Retry-After")
            if retry_after_header and retry_after_header.isdigit():
                response = JSONResponse(
                    status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                    content=_error_payload(
                        "rate_limit_exceeded",
                        f"Rate limit exceeded: {exc.detail}",
                        retry_after=int(retry_after_header),
                    ),
                    headers=dict(response.headers),
                )
        return response

    @app.exception_handler(Exception)
    async def unhandled_exception_handler(_: Request, exc: Exception) -> JSONResponse:
        logger.error("unhandled_exception", error=str(exc))
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content=_error_payload("internal_server_error", "Internal server error"),
        )


def create_app(
    settings: Settings | None = None,
    rate_limit_storage_uri: str | None = None,
) -> FastAPI:
    cfg = settings or get_settings()
    _configure_logging(cfg.ENVIRONMENT)

    configure_webhook_limiter(
        webhook_limiter,
        storage_uri=rate_limit_storage_uri or cfg.REDIS_URL,
        rate_limit=cfg.WEBHOOK_RATE_LIMIT,
    )

    @asynccontextmanager
    async def lifespan(application: FastAPI) -> AsyncIterator[None]:
        owns_db_pool = not hasattr(application.state, "db_pool")
        owns_redis = not hasattr(application.state, "redis")
        owns_kafka = not hasattr(application.state, "kafka_producer")

        if owns_db_pool:
            application.state.db_pool = await create_pg_pool(
                cfg.DATABASE_URL, service="api", min_size=5, max_size=10
            )
        if owns_redis:
            application.state.redis = redis_async.from_url(
                cfg.REDIS_URL, decode_responses=True
            )
        if not hasattr(application.state, "s3_archive"):
            application.state.s3_archive = S3ArchiveService(
                bucket_name=cfg.S3_BUCKET_NAME,
                region=cfg.AWS_REGION,
                endpoint_url=cfg.s3_endpoint_url,
            )
            try:
                await application.state.s3_archive.ensure_bucket()
            except (ClientError, BotoCoreError, OSError) as exc:
                logger.warning("s3_bucket_init_failed", error=str(exc))

        if owns_kafka:
            producer = KafkaEventProducer(cfg.KAFKA_BOOTSTRAP_SERVERS)
            await producer.start()
            application.state.kafka_producer = producer

        try:
            yield
        finally:
            if owns_kafka and hasattr(application.state, "kafka_producer"):
                await application.state.kafka_producer.stop()
            if owns_redis and hasattr(application.state, "redis"):
                await application.state.redis.aclose()
            if owns_db_pool and hasattr(application.state, "db_pool"):
                await application.state.db_pool.close()

    app = FastAPI(title="Engineering Pulse API", version="0.1.0", lifespan=lifespan)
    app.state.settings = cfg
    app.state.limiter = webhook_limiter

    _register_exception_handlers(app)

    app.add_middleware(
        CORSMiddleware,
        allow_origins=cfg.cors_origins,
        allow_credentials=True,
        allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
        allow_headers=["Authorization", "Content-Type"],
    )

    app.include_router(webhooks_router)

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {
            "status": "ok",
            "environment": cfg.ENVIRONMENT,
            "timestamp": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        }

    return app


app = create_app()
