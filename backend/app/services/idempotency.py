from typing import Protocol

SEEN_EVENT_TTL_SECONDS = 86_400


class AsyncRedisLike(Protocol):
    async def set(
        self,
        name: str,
        value: str,
        ex: int | None = None,
        nx: bool = False,
    ) -> bool | None: ...

    async def delete(self, *names: str) -> int: ...


def seen_event_key(event_id: str) -> str:
    return f"seen:events:{event_id}"


async def is_duplicate_event(
    redis_client: AsyncRedisLike,
    event_id: str,
    ttl_seconds: int = SEEN_EVENT_TTL_SECONDS,
) -> bool:
    """Atomically record event_id in Redis; return True if already seen."""
    was_set = await redis_client.set(
        seen_event_key(event_id),
        "1",
        ex=ttl_seconds,
        nx=True,
    )
    return not bool(was_set)


async def clear_seen_event(redis_client: AsyncRedisLike, event_id: str) -> None:
    """Release an idempotency key if downstream persistence/publishing fails."""
    await redis_client.delete(seen_event_key(event_id))

