import json
import uuid
from datetime import UTC, datetime

from fastapi.testclient import TestClient

from app.auth.backend import generate_access_token
from app.config import Settings
from app.main import create_app
from app.models.user import User
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

    user = User(
        id=uuid.uuid4(),
        email="test@pulse.io",
        hashed_password="pw",
        is_active=True,
        is_superuser=False,
        is_verified=True,
        role="member",
    )
    token = generate_access_token(user)

    with TestClient(app) as client, client.websocket_connect("/ws/feed") as ws:
        ws.send_json({"type": "auth", "token": token})
        for _ in range(50):
            if fake_redis.subscribers.get(LIVE_FEED_CHANNEL):
                break
            import time

            time.sleep(0.02)
        assert len(fake_redis.subscribers.get(LIVE_FEED_CHANNEL, [])) == 1

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


async def test_drain_ws_endpoint_closes_active_connections() -> None:
    cfg = Settings(
        ENVIRONMENT="local",
        ES_ENABLED=False,
        GITHUB_WEBHOOK_SECRET="s",
        JIRA_WEBHOOK_SECRET="s",
    )
    app = create_app(settings=cfg, rate_limit_storage_uri="memory://")
    app.state.db_pool = FakeQueryPool()
    app.state.redis = FakeAsyncRedis()
    app.state.s3_archive = FakeS3Archive()
    app.state.kafka_producer = FakeKafkaProducer()

    closed_calls: list[tuple[int, str]] = []

    class FakeWS:
        async def close(self, code: int = 1000, reason: str = "") -> None:
            closed_calls.append((code, reason))

    fake_socket = FakeWS()
    app.state.ws_connections = {fake_socket}

    from httpx import ASGITransport, AsyncClient

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        res = await client.post("/admin/drain-ws")
        assert res.status_code == 200
        assert res.json() == {"status": "drained", "closed_connections": 1}
        assert closed_calls == [(4000, "server restarting")]
