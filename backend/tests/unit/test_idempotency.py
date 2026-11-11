from app.services.idempotency import (
    SEEN_EVENT_TTL_SECONDS,
    clear_seen_event,
    is_duplicate_event,
    seen_event_key,
)
from tests.conftest import FakeAsyncRedis


async def test_idempotency_sets_key_once_and_deduplicates() -> None:
    redis = FakeAsyncRedis()
    event_id = "evt-123"

    first = await is_duplicate_event(redis, event_id)
    second = await is_duplicate_event(redis, event_id)

    assert first is False
    assert second is True
    assert redis.store[seen_event_key(event_id)] == ("1", SEEN_EVENT_TTL_SECONDS)


async def test_clear_seen_event_allows_retry() -> None:
    redis = FakeAsyncRedis()
    event_id = "evt-retry"

    assert await is_duplicate_event(redis, event_id) is False
    await clear_seen_event(redis, event_id)
    assert await is_duplicate_event(redis, event_id) is False

