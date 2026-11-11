import json
from datetime import UTC, datetime
from typing import Any

LIVE_FEED_CHANNEL = "live-feed"


def _to_utc_iso(value: datetime) -> str:
    utc_dt = value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
    return utc_dt.isoformat().replace("+00:00", "Z")


async def publish_live_event(
    redis_client: Any,
    *,
    source: str,
    event_type: str,
    summary: str,
    timestamp: datetime,
) -> None:
    message = {
        "type": "event",
        "source": source,
        "event_type": event_type,
        "summary": summary,
        "timestamp": _to_utc_iso(timestamp),
    }
    await redis_client.publish(LIVE_FEED_CHANNEL, json.dumps(message))

