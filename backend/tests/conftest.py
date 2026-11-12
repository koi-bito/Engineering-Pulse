import asyncio
import hashlib
import hmac
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any

import pytest
from httpx import ASGITransport, AsyncClient

from app.config import Settings
from app.main import create_app


class FakePubSub:
    """In-memory Redis Pub/Sub subscriber used by WebSocket integration tests."""

    def __init__(self, redis: "FakeAsyncRedis") -> None:
        self._redis = redis
        self.subscribed: list[str] = []
        self.unsubscribed: list[str] = []
        self.closed = False
        self._queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self._loop: asyncio.AbstractEventLoop | None = None

    async def subscribe(self, *channels: str) -> None:
        self._loop = asyncio.get_running_loop()
        for channel in channels:
            self.subscribed.append(channel)
            self._redis.subscribers.setdefault(channel, []).append(self)

    async def unsubscribe(self, *channels: str) -> None:
        for channel in channels:
            self.unsubscribed.append(channel)
            subs = self._redis.subscribers.get(channel, [])
            if self in subs:
                subs.remove(self)

    def push_message(self, channel: str, message: str) -> None:
        item = {"type": "message", "channel": channel, "data": message}
        if self._loop is not None and self._loop.is_running():
            self._loop.call_soon_threadsafe(self._queue.put_nowait, item)
        else:
            self._queue.put_nowait(item)

    async def get_message(
        self, ignore_subscribe_messages: bool = False, timeout: float = 0.0
    ) -> dict[str, Any] | None:
        try:
            return await asyncio.wait_for(self._queue.get(), timeout=timeout or 0.05)
        except TimeoutError:
            return None

    async def aclose(self) -> None:
        self.closed = True


class FakeAsyncRedis:
    """In-memory subset of redis.asyncio used in unit and integration tests."""

    def __init__(self) -> None:
        self.store: dict[str, tuple[str, int | None]] = {}
        self.published: list[tuple[str, str]] = []
        self.subscribers: dict[str, list[FakePubSub]] = {}
        self.created_pubsubs: list[FakePubSub] = []

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

    async def get(self, name: str) -> str | None:
        entry = self.store.get(name)
        return entry[0] if entry else None

    async def delete(self, *names: str) -> int:
        deleted = 0
        for name in names:
            if name in self.store:
                del self.store[name]
                deleted += 1
        return deleted

    def publish_sync(self, channel: str, message: str) -> int:
        self.published.append((channel, message))
        subs = list(self.subscribers.get(channel, []))
        for sub in subs:
            sub.push_message(channel, message)
        return len(subs)

    async def publish(self, channel: str, message: str) -> int:
        return self.publish_sync(channel, message)

    def pubsub(self) -> FakePubSub:
        ps = FakePubSub(self)
        self.created_pubsubs.append(ps)
        return ps

    async def aclose(self) -> None:
        pass


class FakeQueryConn:
    """Programmable fake asyncpg connection for read/query integration tests."""

    def __init__(self) -> None:
        self.queries: list[tuple[str, tuple[Any, ...]]] = []
        self.fetchrow_handler: Any = None
        self.fetch_handler: Any = None
        self.fail_next: Exception | None = None

    async def fetchrow(self, query: str, *args: Any) -> Any:
        if self.fail_next is not None:
            exc = self.fail_next
            self.fail_next = None
            raise exc
        self.queries.append((query, args))
        return self.fetchrow_handler(query, *args) if self.fetchrow_handler else None

    async def fetch(self, query: str, *args: Any) -> list[Any]:
        if self.fail_next is not None:
            exc = self.fail_next
            self.fail_next = None
            raise exc
        self.queries.append((query, args))
        return self.fetch_handler(query, *args) if self.fetch_handler else []


class FakeQueryPool:
    def __init__(self) -> None:
        self.conn = FakeQueryConn()

    @asynccontextmanager
    async def acquire(self) -> AsyncIterator[FakeQueryConn]:
        yield self.conn

    async def close(self) -> None:
        pass


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

    async def ensure_bucket(self) -> None:
        pass


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

    async def start(self) -> None:
        pass

    async def stop(self) -> None:
        pass


def sign_github_payload(raw_body: bytes, secret: str) -> str:
    digest = hmac.new(secret.encode("utf-8"), raw_body, hashlib.sha256).hexdigest()
    return f"sha256={digest}"


@pytest.fixture
def fake_redis() -> FakeAsyncRedis:
    return FakeAsyncRedis()


@pytest.fixture
def fake_db_pool() -> FakeQueryPool:
    return FakeQueryPool()


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
