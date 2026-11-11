import asyncio
import json
from datetime import UTC, datetime
from typing import Any

import structlog

from app.config import get_settings
from app.consumers.base import (
    DLQ_GROUP_ID,
    DLQ_TOPIC,
    BaseKafkaConsumer,
    install_signal_handlers,
)
from app.db.pool import acquire_connection, create_pg_pool
from app.db.repositories import insert_failed_event

logger = structlog.get_logger(__name__)


async def process_dlq_message(
    raw_value: bytes,
    *,
    db_pool: Any,
    service_name: str = "dlq-consumer",
) -> None:
    payload = json.loads(raw_value.decode("utf-8"))
    failed_at_raw = payload.get("failed_at")
    failed_at = (
        datetime.fromisoformat(failed_at_raw).astimezone(UTC)
        if isinstance(failed_at_raw, str)
        else datetime.now(UTC)
    )

    async with acquire_connection(db_pool, service_name) as conn, conn.transaction():
        await insert_failed_event(
            conn,
            event_id=payload.get("event_id"),
            source=payload.get("source"),
            topic=str(payload.get("topic") or "unknown"),
            error=str(payload.get("error") or "unknown error"),
            raw_payload=payload.get("raw_payload") or {},
            failed_at=failed_at,
        )
    logger.warning(
        "dlq_event_stored",
        event_id=payload.get("event_id"),
        topic=payload.get("topic"),
    )


async def run() -> None:
    settings = get_settings()
    db_pool = await create_pg_pool(
        settings.DATABASE_URL, service="dlq-consumer", min_size=1, max_size=2
    )
    consumer = BaseKafkaConsumer(
        topics=[DLQ_TOPIC],
        group_id=DLQ_GROUP_ID,
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        dlq_topic=None,
        handler=lambda record: process_dlq_message(record.value, db_pool=db_pool),
    )
    install_signal_handlers(consumer)
    try:
        await consumer.run_forever()
    finally:
        await db_pool.close()


if __name__ == "__main__":
    asyncio.run(run())
