from functools import lru_cache
from typing import Literal

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    ENVIRONMENT: Literal["local", "production"] = "local"

    # Single source of truth for database connection; never derived from ENVIRONMENT.
    DATABASE_URL: str = "postgresql+asyncpg://postgres:postgres@localhost:5432/engineering_pulse"
    REDIS_URL: str = "redis://localhost:6379/0"
    KAFKA_BOOTSTRAP_SERVERS: str = "localhost:9092"
    ELASTICSEARCH_URL: str = "http://localhost:9200"
    ES_ENABLED: bool = True

    AWS_REGION: str = "us-east-1"
    AWS_ENDPOINT_URL: str | None = "http://localhost:4566"
    S3_BUCKET_NAME: str = "engineering-pulse-raw-events"

    CORS_ORIGINS: str = "http://localhost:5173"

    GITHUB_WEBHOOK_SECRET: str = ""
    JIRA_WEBHOOK_SECRET: str = ""
    WEBHOOK_RATE_LIMIT: str = "100/minute"

    model_config = SettingsConfigDict(
        env_file=(".env", "../.env"),
        env_file_encoding="utf-8",
        extra="ignore",
    )

    @property
    def cors_origins(self) -> list[str]:
        origins = [o.strip() for o in self.CORS_ORIGINS.split(",") if o.strip()]
        if self.ENVIRONMENT == "local" and "http://127.0.0.1:5173" not in origins:
            origins.append("http://127.0.0.1:5173")
        return origins

    @property
    def s3_endpoint_url(self) -> str | None:
        return self.AWS_ENDPOINT_URL if self.ENVIRONMENT == "local" else None


@lru_cache
def get_settings() -> Settings:
    return Settings()

