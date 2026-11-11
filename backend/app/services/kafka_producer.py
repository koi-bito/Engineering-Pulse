import json
from typing import Any

import structlog
from aiokafka import AIOKafkaProducer

from app.schemas.events import WebhookEnvelope

logger = structlog.get_logger(__name__)

TOPIC_BY_SOURCE = {
    "github": "github-events",
    "jira": "jira-events",
}


class KafkaEventProducer:
    """Async Kafka producer wrapper with explicit startup error logging."""

    def __init__(
        self,
        bootstrap_servers: str,
        producer: Any | None = None,
    ) -> None:
        self.bootstrap_servers = bootstrap_servers
        self._producer = producer or AIOKafkaProducer(
            bootstrap_servers=bootstrap_servers,
            request_timeout_ms=30_000,
            retry_backoff_ms=500,
            acks="all",
        )
        self._started = False

    async def start(self) -> None:
        try:
            await self._producer.start()
            self._started = True
        except Exception as exc:
            logger.error(
                "kafka_producer_startup_failed",
                bootstrap_servers=self.bootstrap_servers,
                error=str(exc),
            )
            raise

    async def stop(self) -> None:
        if self._started:
            await self._producer.stop()
            self._started = False

    async def publish_event(self, envelope: WebhookEnvelope) -> str:
        topic = TOPIC_BY_SOURCE[envelope.source]
        encoded = json.dumps(envelope.model_dump(mode="json")).encode("utf-8")
        key = envelope.event_id.encode("utf-8")
        try:
            # send_and_wait awaits broker ack per message, preserving ordering on retry.
            await self._producer.send_and_wait(topic, value=encoded, key=key)
        except Exception as exc:
            logger.error(
                "kafka_publish_failed",
                event_id=envelope.event_id,
                source=envelope.source,
                topic=topic,
                error=str(exc),
            )
            raise
        return topic

