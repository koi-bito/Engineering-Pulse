import json
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from typing import Any

import pytest
from elasticsearch import NotFoundError
from httpx import ASGITransport, AsyncClient

from app.config import Settings
from app.main import create_app
from app.services.cache import (
    AVG_CYCLE_TIME_7D_KEY,
    METRICS_SUMMARY_KEY,
    PRS_MERGED_TODAY_KEY,
    sprint_active_key,
)
from tests.conftest import (
    FakeAsyncRedis,
    FakeKafkaProducer,
    FakeQueryPool,
    FakeS3Archive,
    sign_github_payload,
)


class FakeSearchESClient:
    def __init__(self, response: dict[str, Any] | None = None, not_found: bool = False) -> None:
        self.response = response or {"hits": {"total": {"value": 0}, "hits": []}}
        self.not_found = not_found
        self.search_calls: list[dict[str, Any]] = []

    async def search(self, **kwargs: Any) -> dict[str, Any]:
        self.search_calls.append(kwargs)
        if self.not_found:
            raise NotFoundError("index_not_found_exception", meta=None, body=None)
        return self.response

    async def close(self) -> None:
        pass


@pytest.fixture
def fake_es_client() -> FakeSearchESClient:
    return FakeSearchESClient()


@pytest.fixture
async def api_client(
    test_settings: Settings,
    fake_db_pool: FakeQueryPool,
    fake_redis: FakeAsyncRedis,
    fake_s3: FakeS3Archive,
    fake_kafka: FakeKafkaProducer,
    fake_es_client: FakeSearchESClient,
) -> AsyncIterator[AsyncClient]:
    app = create_app(settings=test_settings, rate_limit_storage_uri="memory://")
    app.state.db_pool = fake_db_pool
    app.state.redis = fake_redis
    app.state.s3_archive = fake_s3
    app.state.kafka_producer = fake_kafka
    app.state.es_client = fake_es_client

    async with AsyncClient(
        transport=ASGITransport(app=app, raise_app_exceptions=False),
        base_url="http://test",
    ) as client:
        yield client


async def test_metrics_summary_reads_redis_first_and_populates_keys(
    api_client: AsyncClient,
    fake_db_pool: FakeQueryPool,
    fake_redis: FakeAsyncRedis,
) -> None:
    fake_db_pool.conn.fetchrow_handler = lambda _q, *_a: {
        "prs_merged_today": 6,
        "avg_cycle_time_7d_hours": 11.5,
        "avg_ticket_cycle_time_7d_hours": 22.0,
        "open_prs": 4,
        "active_tickets": 9,
        "commits_today": 17,
    }

    first = await api_client.get("/api/metrics/summary")
    assert first.status_code == 200
    assert first.json()["prs_merged_today"] == 6
    assert first.json()["avg_cycle_time_7d_hours"] == 11.5
    assert len(fake_db_pool.conn.queries) == 1
    assert await fake_redis.get(PRS_MERGED_TODAY_KEY) == "6"
    assert await fake_redis.get(AVG_CYCLE_TIME_7D_KEY) == "11.5"
    assert await fake_redis.get(METRICS_SUMMARY_KEY) is not None

    # Second request must hit Redis without querying Postgres again.
    fake_db_pool.conn.fail_next = RuntimeError("db should not be called on cache hit")
    second = await api_client.get("/api/metrics/summary")
    assert second.status_code == 200
    assert second.json() == first.json()
    assert len(fake_db_pool.conn.queries) == 1


async def test_pr_and_cycle_time_endpoints_validate_range_and_cache(
    api_client: AsyncClient,
    fake_db_pool: FakeQueryPool,
) -> None:
    def fetch_handler(query: str, *args: Any) -> list[dict[str, Any]]:
        if "merged_count" in query:
            return [{"date": "2026-11-11", "merged_count": 3, "opened_count": 5}]
        return [
            {
                "date": "2026-11-11",
                "avg_ticket_cycle_time_hours": 18.0,
                "rolling_avg_hours": 16.5,
                "avg_pr_cycle_time_hours": 7.2,
                "resolved_tickets": 2,
                "merged_prs": 3,
            }
        ]

    fake_db_pool.conn.fetch_handler = fetch_handler

    prs_res = await api_client.get("/api/metrics/prs?range=14d")
    assert prs_res.status_code == 200
    assert prs_res.json()["days"] == 14
    assert prs_res.json()["series"][0]["merged_count"] == 3

    ct_res = await api_client.get("/api/metrics/cycle-time?range=30d")
    assert ct_res.status_code == 200
    assert ct_res.json()["days"] == 30
    assert ct_res.json()["series"][0]["rolling_avg_hours"] == 16.5

    bad = await api_client.get("/api/metrics/prs?range=999d")
    assert bad.status_code == 400
    assert bad.json()["error"] == "bad_request"


async def test_sprint_active_caches_with_60s_ttl(
    api_client: AsyncClient,
    fake_db_pool: FakeQueryPool,
    fake_redis: FakeAsyncRedis,
) -> None:
    def fetch_handler(query: str, *args: Any) -> list[dict[str, Any]]:
        if "GROUP BY status" in query:
            return [{"status": "In Progress", "count": 2}, {"status": "Done", "count": 1}]
        return [
            {
                "id": "ENG-1",
                "project": "ENG",
                "assignee": "alice",
                "summary": "Build WS feed",
                "status": "In Progress",
                "created_at": datetime(2026, 11, 11, 8, 0, tzinfo=UTC),
                "resolved_at": None,
                "cycle_time_hours": None,
            }
        ]

    fake_db_pool.conn.fetch_handler = fetch_handler

    resp = await api_client.get("/api/sprint/active?project=ENG")
    assert resp.status_code == 200
    body = resp.json()
    assert body["project"] == "ENG"
    assert body["status_counts"] == {"In Progress": 2, "Done": 1}
    assert body["tickets"][0]["created_at"] == "2026-11-11T08:00:00Z"
    assert fake_redis.store[sprint_active_key("ENG")][1] == 60


async def test_search_uses_elasticsearch_and_handles_missing_index(
    api_client: AsyncClient,
    fake_es_client: FakeSearchESClient,
) -> None:
    fake_es_client.response = {
        "hits": {
            "total": {"value": 1, "relation": "eq"},
            "hits": [
                {
                    "_id": "pr:42",
                    "_score": 2.4,
                    "_source": {
                        "type": "pull_request",
                        "source": "github",
                        "author": "kunal",
                        "repo": "acme/api",
                        "timestamp": "2026-11-11T10:00:00Z",
                        "title": "Fix auth refresh race",
                    },
                }
            ],
        }
    }

    res = await api_client.get("/api/search?q=auth")
    assert res.status_code == 200
    body = res.json()
    assert body["total_hits"] == 1
    assert body["results"][0]["snippet"] == "Fix auth refresh race"
    assert len(fake_es_client.search_calls) == 1

    fake_es_client.not_found = True
    empty_res = await api_client.get("/api/search?q=unindexed")
    assert empty_res.status_code == 200
    assert empty_res.json()["total_hits"] == 0

    blank = await api_client.get("/api/search?q=%20%20")
    assert blank.status_code == 400
    assert blank.json()["error"] == "bad_request"


async def test_search_falls_back_to_postgres_when_es_disabled(
    fake_db_pool: FakeQueryPool,
    fake_redis: FakeAsyncRedis,
    fake_s3: FakeS3Archive,
    fake_kafka: FakeKafkaProducer,
) -> None:
    cfg = Settings(
        ENVIRONMENT="local",
        ES_ENABLED=False,
        GITHUB_WEBHOOK_SECRET="s",
        JIRA_WEBHOOK_SECRET="s",
    )
    app = create_app(settings=cfg, rate_limit_storage_uri="memory://")
    app.state.db_pool = fake_db_pool
    app.state.redis = fake_redis
    app.state.s3_archive = fake_s3
    app.state.kafka_producer = fake_kafka
    app.state.es_client = None

    fake_db_pool.conn.fetch_handler = lambda _q, *_a: [
        {
            "id": "commit:abc",
            "type": "commit",
            "source": "github",
            "author": "alice",
            "repo": "acme/api",
            "timestamp": datetime(2026, 11, 11, 9, 0, tzinfo=UTC),
            "snippet": "fix: auth bug",
        }
    ]

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        res = await client.get("/api/search?q=auth")

    assert res.status_code == 200
    assert res.json()["results"][0]["id"] == "commit:abc"


async def test_consistent_error_shapes_and_metrics_endpoint(
    api_client: AsyncClient,
    test_settings: Settings,
    fake_db_pool: FakeQueryPool,
) -> None:
    not_found = await api_client.get("/api/does-not-exist")
    assert not_found.status_code == 404
    assert not_found.json()["error"] == "not_found"

    missing_q = await api_client.get("/api/search")
    assert missing_q.status_code == 422
    assert missing_q.json()["error"] == "validation_error"

    fake_db_pool.conn.fail_next = RuntimeError("db exploded")
    boom = await api_client.get("/api/metrics/summary")
    assert boom.status_code == 500
    assert boom.json() == {
        "error": "internal_server_error",
        "detail": "Internal server error",
    }

    # Trigger webhook counter & cache ratio, then scrape /metrics.
    body = json.dumps({"action": "opened"}).encode("utf-8")
    sig = sign_github_payload(body, test_settings.GITHUB_WEBHOOK_SECRET)
    await api_client.post(
        "/webhooks/github",
        content=body,
        headers={
            "X-Hub-Signature-256": sig,
            "X-GitHub-Delivery": "metrics-evt-1",
            "X-GitHub-Event": "pull_request",
        },
    )

    metrics = await api_client.get("/metrics")
    assert metrics.status_code == 200
    text = metrics.text
    assert "webhook_events_received_total" in text
    assert "cache_hit_ratio" in text
    assert "http_requests_total" in text

