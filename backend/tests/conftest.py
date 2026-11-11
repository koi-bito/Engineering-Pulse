import hashlib
import hmac
from collections.abc import AsyncIterator
from datetime import datetime
from typing import Any

import pytest
from httpx import ASGITransport, AsyncClient

from app.config import Settings
from app.main import create_app


class FakeAsyncRedis:
    """In-memory subset of redis.asyncio used in unit and integration tests."""

    def __init__(self) -> None:
        self.store: dict[str, tuple[str, int | None]] = {}

    async def set(
        self,
        name: str,
        value: str,
        ex: int | None = None,
        nx: bool = False,
    ) -> bool | None:
        if nx and name in self.store:
            return None
        self.store[name] = (value, ex)
        return True

    async def delete(self, *names: str) -> int:
        deleted = 0
        for name in names:
            if name in self.store:
                del self.store[name]
                deleted += 1
        return deleted


class FakeS3Archive:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, datetime, bytes]] = []
        self.fail_next = False

    async def archive_raw_event(
        self,
        source: str,
        event_id: str,
        received_at: datetime,
        raw_body: bytes,
    ) -> str:
        if self.fail_next:
            self.fail_next = False
            raise RuntimeError("simulated s3 failure")
        self.calls.append((source, event_id, received_at, raw_body))
        return f"raw/{source}/{received_at:%Y/%m/%d}/{event_id}.json"


class FakeKafkaProducer:
    def __init__(self) -> None:
        self.published: list[Any] = []
        self.fail_next = False

    async def publish_event(self, envelope: Any) -> str:
        if self.fail_next:
            self.fail_next = False
            raise RuntimeError("simulated kafka failure")
        self.published.append(envelope)
        return "github-events" if envelope.source == "github" else "jira-events"


def sign_github_payload(raw_body: bytes, secret: str) -> str:
    digest = hmac.new(secret.encode("utf-8"), raw_body, hashlib.sha256).hexdigest()
    return f"sha256={digest}"


@pytest.fixture
def fake_redis() -> FakeAsyncRedis:
    return FakeAsyncRedis()


@pytest.fixture
def fake_s3() -> FakeS3Archive:
    return FakeS3Archive()


@pytest.fixture
def fake_kafka() -> FakeKafkaProducer:
    return FakeKafkaProducer()


@pytest.fixture
def test_settings() -> Settings:
    return Settings(
        ENVIRONMENT="local",
        GITHUB_WEBHOOK_SECRET="test-github-secret",
        JIRA_WEBHOOK_SECRET="test-jira-secret",
        WEBHOOK_RATE_LIMIT="100/minute",
    )


@pytest.fixture
async def webhook_client(
    test_settings: Settings,
    fake_redis: FakeAsyncRedis,
    fake_s3: FakeS3Archive,
    fake_kafka: FakeKafkaProducer,
) -> AsyncIterator[AsyncClient]:
    app = create_app(settings=test_settings, rate_limit_storage_uri="memory://")
    app.state.redis = fake_redis
    app.state.s3_archive = fake_s3
    app.state.kafka_producer = fake_kafka

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        yield client

