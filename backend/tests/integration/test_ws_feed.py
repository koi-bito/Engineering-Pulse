import json
from datetime import UTC, datetime

from fastapi.testclient import TestClient

from app.config import Settings
from app.main import create_app
from app.services.pubsub import LIVE_FEED_CHANNEL, publish_live_event
from tests.conftest import FakeAsyncRedis, FakeKafkaProducer, FakeQueryPool, FakeS3Archive


def test_ws_feed_streams_redis_pubsub_and_cleans_up_on_disconnect() -> None:
    cfg = Settings(
        ENVIRONMENT="local",
        ES_ENABLED=False,
        GITHUB_WEBHOOK_SECRET="s",
        JIRA_WEBHOOK_SECRET="s",
    )
    fake_redis = FakeAsyncRedis()
    app = create_app(settings=cfg, rate_limit_storage_uri="memory://")
    app.state.db_pool = FakeQueryPool()
    app.state.redis = fake_redis
    app.state.s3_archive = FakeS3Archive()
    app.state.kafka_producer = FakeKafkaProducer()
    app.state.es_client = None

    with TestClient(app) as client, client.websocket_connect("/ws/feed") as ws:
        ws.send_json({"type": "auth", "token": "phase-1-placeholder"})
        assert len(fake_redis.subscribers[LIVE_FEED_CHANNEL]) == 1

        fake_redis.publish_sync(
            LIVE_FEED_CHANNEL,
            json.dumps(
                {
                    "type": "event",
                    "source": "github",
                    "event_type": "pull_request",
                    "summary": "[acme/api] PR closed: Add WS feed",
                    "timestamp": "2026-11-11T10:00:00Z",
                }
            ),
        )
        msg1 = ws.receive_json()
        assert msg1["type"] == "event"
        assert msg1["source"] == "github"
        assert msg1["event_type"] == "pull_request"
        assert msg1["data"]["summary"] == "[acme/api] PR closed: Add WS feed"

    assert fake_redis.subscribers[LIVE_FEED_CHANNEL] == []
    assert fake_redis.created_pubsubs[0].unsubscribed == [LIVE_FEED_CHANNEL]
    assert fake_redis.created_pubsubs[0].closed is True
    assert len(app.state.ws_connections) == 0


async def test_publish_live_event_includes_protocol_data_field() -> None:
    redis = FakeAsyncRedis()
    await publish_live_event(
        redis,
        source="jira",
        event_type="ticket",
        summary="[ENG-1] Done: Fix lag",
        timestamp=datetime(2026, 11, 11, 10, 0, tzinfo=UTC),
    )
    assert len(redis.published) == 1
    channel, raw = redis.published[0]
    assert channel == LIVE_FEED_CHANNEL
    payload = json.loads(raw)
    assert payload["type"] == "event"
    assert payload["data"]["summary"] == "[ENG-1] Done: Fix lag"
    assert payload["data"]["timestamp"] == "2026-11-11T10:00:00Z"
