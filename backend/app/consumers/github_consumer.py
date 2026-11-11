import asyncio
import json
from typing import Any

import redis.asyncio as redis_async
import structlog

from app.config import get_settings
from app.consumers.base import CONSUMER_GROUP_ID, BaseKafkaConsumer, install_signal_handlers
from app.db.pool import acquire_connection, create_pg_pool
from app.db.repositories import upsert_commit, upsert_pull_request
from app.schemas.events import (
    WebhookEnvelope,
    normalize_github_commits,
    normalize_github_pr,
)
from app.services.cache import invalidate_metrics_cache
from app.services.es_index import ElasticsearchIndexer
from app.services.pubsub import publish_live_event

logger = structlog.get_logger(__name__)

GITHUB_TOPIC = "github-events"


async def process_github_message(
    raw_value: bytes,
    *,
    db_pool: Any,
    es_indexer: ElasticsearchIndexer,
    redis_client: Any,
    service_name: str = "github-consumer",
) -> None:
    envelope = WebhookEnvelope.model_validate(json.loads(raw_value.decode("utf-8")))

    if envelope.type == "pull_request":
        pr = normalize_github_pr(envelope.payload)
        async with acquire_connection(db_pool, service_name) as conn, conn.transaction():
            await upsert_pull_request(conn, pr)

        await es_indexer.index_event(
            f"pr:{pr.id}",
            {
                "title": pr.title,
                "author": pr.author,
                "repo": pr.repo,
                "type": "pull_request",
                "source": "github",
                "timestamp": pr.opened_at.isoformat().replace("+00:00", "Z"),
            },
        )
        await invalidate_metrics_cache(redis_client)
        await publish_live_event(
            redis_client,
            source="github",
            event_type="pull_request",
            summary=f"[{pr.repo}] PR {pr.state}: {pr.title}",
            timestamp=pr.merged_at or pr.opened_at,
        )
        logger.info("github_pr_processed", event_id=envelope.event_id, pr_id=str(pr.id))
        return

    if envelope.type == "push":
        commits = normalize_github_commits(envelope.payload)
        async with acquire_connection(db_pool, service_name) as conn, conn.transaction():
            for commit in commits:
                await upsert_commit(conn, commit)

        for commit in commits:
            await es_indexer.index_event(
                f"commit:{commit.sha}",
                {
                    "message": commit.message,
                    "author": commit.author,
                    "repo": commit.repo,
                    "type": "commit",
                    "source": "github",
                    "timestamp": commit.timestamp.isoformat().replace("+00:00", "Z"),
                },
            )
            await publish_live_event(
                redis_client,
                source="github",
                event_type="commit",
                summary=f"[{commit.repo}] {commit.author}: {commit.message[:80]}",
                timestamp=commit.timestamp,
            )
        logger.info(
            "github_push_processed", event_id=envelope.event_id, commit_count=len(commits)
        )
        return

    logger.info("github_event_skipped", event_id=envelope.event_id, event_type=envelope.type)


async def run() -> None:
    settings = get_settings()
    db_pool = await create_pg_pool(
        settings.DATABASE_URL, service="github-consumer", min_size=2, max_size=5
    )
    redis_client = redis_async.from_url(settings.REDIS_URL, decode_responses=True)
    es_indexer = ElasticsearchIndexer(settings.ELASTICSEARCH_URL, enabled=settings.ES_ENABLED)
    await es_indexer.ensure_index()

    consumer = BaseKafkaConsumer(
        topics=[GITHUB_TOPIC],
        group_id=CONSUMER_GROUP_ID,
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        handler=lambda record: process_github_message(
            record.value,
            db_pool=db_pool,
            es_indexer=es_indexer,
            redis_client=redis_client,
        ),
    )
    install_signal_handlers(consumer)
    try:
        await consumer.run_forever()
    finally:
        await es_indexer.close()
        await redis_client.aclose()
        await db_pool.close()


if __name__ == "__main__":
    asyncio.run(run())
