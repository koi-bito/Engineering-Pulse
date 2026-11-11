import asyncio
import json
from typing import Any

import redis.asyncio as redis_async
import structlog

from app.config import get_settings
from app.consumers.base import CONSUMER_GROUP_ID, BaseKafkaConsumer, install_signal_handlers
from app.db.pool import acquire_connection, create_pg_pool
from app.db.repositories import insert_jira_transition, upsert_jira_ticket
from app.schemas.events import (
    WebhookEnvelope,
    normalize_jira_ticket,
    normalize_jira_transitions,
)
from app.services.cache import invalidate_metrics_cache
from app.services.es_index import ElasticsearchIndexer
from app.services.pubsub import publish_live_event

logger = structlog.get_logger(__name__)

JIRA_TOPIC = "jira-events"


async def process_jira_message(
    raw_value: bytes,
    *,
    db_pool: Any,
    es_indexer: ElasticsearchIndexer,
    redis_client: Any,
    service_name: str = "jira-consumer",
) -> None:
    envelope = WebhookEnvelope.model_validate(json.loads(raw_value.decode("utf-8")))
    if not envelope.type.startswith("jira:issue_"):
        logger.info("jira_event_skipped", event_id=envelope.event_id, event_type=envelope.type)
        return

    ticket = normalize_jira_ticket(envelope.payload)
    transitions = normalize_jira_transitions(
        envelope.payload, fallback_timestamp=envelope.received_at
    )

    async with acquire_connection(db_pool, service_name) as conn, conn.transaction():
        await upsert_jira_ticket(conn, ticket)
        for transition in transitions:
            await insert_jira_transition(conn, transition)

    await es_indexer.index_event(
        f"ticket:{ticket.id}",
        {
            "summary": ticket.summary,
            "author": ticket.assignee or "unassigned",
            "repo": ticket.project,
            "type": "ticket",
            "source": "jira",
            "timestamp": ticket.created_at.isoformat().replace("+00:00", "Z"),
        },
    )
    await invalidate_metrics_cache(redis_client, project_key=ticket.project)
    await publish_live_event(
        redis_client,
        source="jira",
        event_type="ticket",
        summary=f"[{ticket.id}] {ticket.status}: {ticket.summary}",
        timestamp=ticket.resolved_at or ticket.created_at,
    )
    logger.info(
        "jira_ticket_processed",
        event_id=envelope.event_id,
        ticket_id=ticket.id,
        transitions=len(transitions),
    )


async def run() -> None:
    settings = get_settings()
    db_pool = await create_pg_pool(
        settings.DATABASE_URL, service="jira-consumer", min_size=2, max_size=5
    )
    redis_client = redis_async.from_url(settings.REDIS_URL, decode_responses=True)
    es_indexer = ElasticsearchIndexer(settings.ELASTICSEARCH_URL, enabled=settings.ES_ENABLED)
    await es_indexer.ensure_index()

    consumer = BaseKafkaConsumer(
        topics=[JIRA_TOPIC],
        group_id=CONSUMER_GROUP_ID,
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        handler=lambda record: process_jira_message(
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
