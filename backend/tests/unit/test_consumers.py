import json
from contextlib import asynccontextmanager
from types import SimpleNamespace

from aiokafka import TopicPartition

from app.consumers.base import BaseKafkaConsumer
from app.consumers.dlq_consumer import process_dlq_message
from app.consumers.github_consumer import process_github_message
from app.consumers.jira_consumer import process_jira_message
from app.services.cache import PRS_MERGED_TODAY_KEY, sprint_active_key
from app.services.es_index import ElasticsearchIndexer
from app.services.pubsub import LIVE_FEED_CHANNEL
from tests.conftest import FakeAsyncRedis
from tests.unit.test_cache_and_es import FakeESClient


class FakeConn:
    def __init__(self) -> None:
        self.executed: list[tuple[str, tuple]] = []

    async def execute(self, query: str, *args) -> None:
        self.executed.append((query, args))

    @asynccontextmanager
    async def transaction(self):
        yield self


class FakePool:
    def __init__(self) -> None:
        self.conn = FakeConn()

    @asynccontextmanager
    async def acquire(self):
        yield self.conn


class RecordingRedis(FakeAsyncRedis):
    def __init__(self) -> None:
        super().__init__()
        self.published: list[tuple[str, str]] = []

    async def get(self, name: str) -> str | None:
        entry = self.store.get(name)
        return entry[0] if entry else None

    async def publish(self, channel: str, message: str) -> int:
        self.published.append((channel, message))
        return 1


class FakeKafkaConsumer:
    def __init__(self) -> None:
        self.committed: list[dict] = []
        self.subscribed: list[str] = []

    def subscribe(self, topics, listener=None) -> None:
        self.subscribed = list(topics)

    async def start(self) -> None:
        pass

    async def stop(self) -> None:
        pass

    async def commit(self, offsets=None) -> None:
        self.committed.append(offsets)


class FakeDLQProducer:
    def __init__(self, fail: bool = False) -> None:
        self.sent: list[dict] = []
        self.fail = fail

    async def start(self) -> None:
        pass

    async def stop(self) -> None:
        pass

    async def send_and_wait(self, topic: str, value: bytes, key: bytes | None = None) -> None:
        if self.fail:
            raise RuntimeError("dlq broker down")
        self.sent.append({"topic": topic, "value": json.loads(value.decode("utf-8")), "key": key})


async def test_process_github_pr_writes_pg_es_cache_and_pubsub() -> None:
    pool = FakePool()
    redis = RecordingRedis()
    await redis.set(PRS_MERGED_TODAY_KEY, "5", ex=300)
    es_client = FakeESClient()
    es = ElasticsearchIndexer("http://localhost:9200", enabled=True, client=es_client)

    envelope = {
        "event_id": "evt-pr-1",
        "source": "github",
        "type": "pull_request",
        "received_at": "2026-11-11T10:00:00Z",
        "payload": {
            "repository": {"full_name": "acme/api"},
            "pull_request": {
                "id": 77,
                "title": "Add consumer",
                "state": "closed",
                "user": {"login": "kunal"},
                "created_at": "2026-11-11T08:00:00Z",
                "merged_at": "2026-11-11T10:00:00Z",
            },
        },
    }

    await process_github_message(
        json.dumps(envelope).encode("utf-8"),
        db_pool=pool,
        es_indexer=es,
        redis_client=redis,
    )

    assert len(pool.conn.executed) == 1
    assert "INSERT INTO pull_requests" in pool.conn.executed[0][0]
    assert len(es_client.indexed) == 1
    assert PRS_MERGED_TODAY_KEY not in redis.store
    assert len(redis.published) == 1
    channel, raw_msg = redis.published[0]
    assert channel == LIVE_FEED_CHANNEL
    msg = json.loads(raw_msg)
    assert msg["type"] == "event"
    assert msg["source"] == "github"
    assert msg["event_type"] == "pull_request"


async def test_process_jira_ticket_with_es_disabled_writes_pg_and_skips_es() -> None:
    pool = FakePool()
    redis = RecordingRedis()
    await redis.set(sprint_active_key("ENG"), "[]", ex=60)
    es_client = FakeESClient()
    es = ElasticsearchIndexer("http://localhost:9200", enabled=False, client=es_client)

    envelope = {
        "event_id": "evt-jira-1",
        "source": "jira",
        "type": "jira:issue_updated",
        "received_at": "2026-11-11T10:00:00Z",
        "payload": {
            "webhookEvent": "jira:issue_updated",
            "issue": {
                "key": "ENG-9",
                "fields": {
                    "project": {"key": "ENG"},
                    "summary": "Tune consumer",
                    "status": {"name": "Done"},
                    "assignee": {"displayName": "kunal"},
                    "created": "2026-11-11T08:00:00Z",
                    "resolutiondate": "2026-11-11T10:00:00Z",
                },
            },
            "changelog": {
                "items": [
                    {"field": "status", "fromString": "In Progress", "toString": "Done"}
                ]
            },
        },
    }

    await process_jira_message(
        json.dumps(envelope).encode("utf-8"),
        db_pool=pool,
        es_indexer=es,
        redis_client=redis,
    )

    assert len(pool.conn.executed) == 2
    assert "INSERT INTO jira_tickets" in pool.conn.executed[0][0]
    assert "INSERT INTO jira_transitions" in pool.conn.executed[1][0]
    assert es_client.indexed == []
    assert sprint_active_key("ENG") not in redis.store
    assert len(redis.published) == 1


async def test_base_consumer_commits_after_success_and_routes_failures_to_dlq() -> None:
    kafka_consumer = FakeKafkaConsumer()
    dlq_producer = FakeDLQProducer()

    async def handler(record) -> None:
        if record.value == b"bad":
            raise ValueError("invalid payload")

    consumer = BaseKafkaConsumer(
        topics=["github-events"],
        group_id="engineering-pulse-consumers",
        bootstrap_servers="localhost:9092",
        handler=handler,
        consumer=kafka_consumer,
        dlq_producer=dlq_producer,
    )

    good_record = SimpleNamespace(topic="github-events", partition=1, offset=10, value=b"ok")
    bad_record = SimpleNamespace(
        topic="github-events",
        partition=1,
        offset=11,
        value=json.dumps({"event_id": "bad-1", "source": "github"}).encode("utf-8"),
    )
    # Force failure on raw invalid value for second test case
    bad_raw = SimpleNamespace(topic="github-events", partition=1, offset=11, value=b"bad")

    await consumer.process_record(good_record)
    assert kafka_consumer.committed[-1] == {TopicPartition("github-events", 1): 11}

    await consumer.process_record(bad_raw)
    assert len(dlq_producer.sent) == 1
    assert dlq_producer.sent[0]["topic"] == "events-dlq"
    assert dlq_producer.sent[0]["value"]["error"] == "invalid payload"
    assert kafka_consumer.committed[-1] == {TopicPartition("github-events", 1): 12}
    assert bad_record.offset == 11


async def test_dlq_consumer_persists_failed_event_to_postgres() -> None:
    pool = FakePool()
    dlq_msg = json.dumps(
        {
            "event_id": "evt-fail",
            "source": "github",
            "topic": "github-events",
            "error": "ValidationError",
            "raw_payload": {"broken": True},
            "failed_at": "2026-11-11T10:00:00Z",
        }
    ).encode("utf-8")

    await process_dlq_message(dlq_msg, db_pool=pool)

    assert len(pool.conn.executed) == 1
    assert "INSERT INTO failed_events" in pool.conn.executed[0][0]

