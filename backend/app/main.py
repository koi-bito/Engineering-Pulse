import logging
from datetime import UTC, datetime

import structlog
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.config import get_settings


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


def create_app() -> FastAPI:
    settings = get_settings()
    _configure_logging(settings.ENVIRONMENT)

    app = FastAPI(title="Engineering Pulse API", version="0.1.0")

    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origins,
        allow_credentials=True,
        allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
        allow_headers=["Authorization", "Content-Type"],
    )

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {
            "status": "ok",
            "environment": settings.ENVIRONMENT,
            "timestamp": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        }

    return app


app = create_app()
