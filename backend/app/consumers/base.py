import asyncio
import json
import signal
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import Any

import structlog
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, ConsumerRebalanceListener, TopicPartition

logger = structlog.get_logger(__name__)

CONSUMER_GROUP_ID = "engineering-pulse-consumers"
DLQ_GROUP_ID = "engineering-pulse-dlq-consumers"
DLQ_TOPIC = "events-dlq"


class TrackingRebalanceListener(ConsumerRebalanceListener):
    """Commits tracked offsets before partition revocation to minimize duplicate replays."""

    def __init__(self, consumer: AIOKafkaConsumer) -> None:
        self._consumer = consumer
        self.pending_commits: dict[TopicPartition, int] = {}

    async def on_partitions_revoked(self, revoked: list[TopicPartition]) -> None:
        offsets = {tp: off for tp, off in self.pending_commits.items() if tp in revoked}
        if offsets:
            await self._consumer.commit(offsets)
            for tp in offsets:
                self.pending_commits.pop(tp, None)
        logger.info(
            "kafka_partitions_revoked",
            partitions=[f"{tp.topic}:{tp.partition}" for tp in revoked],
        )

    async def on_partitions_assigned(self, assigned: list[TopicPartition]) -> None:
        logger.info(
            "kafka_partitions_assigned",
            partitions=[f"{tp.topic}:{tp.partition}" for tp in assigned],
        )


class BaseKafkaConsumer:
    """Shared manual-commit Kafka consumer loop with DLQ fallback and clean shutdown."""

    def __init__(
        self,
        *,
        topics: list[str],
        group_id: str,
        bootstrap_servers: str,
        handler: Callable[[Any], Awaitable[None]],
        dlq_topic: str | None = DLQ_TOPIC,
        consumer: Any | None = None,
        dlq_producer: Any | None = None,
    ) -> None:
        self.topics = topics
        self.group_id = group_id
        self.bootstrap_servers = bootstrap_servers
        self._handler = handler
        self.dlq_topic = dlq_topic
        self._stop_event = asyncio.Event()
        self._consumer = consumer or AIOKafkaConsumer(
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
        )
        self._dlq_producer = (
            dlq_producer
            if dlq_producer is not None
            else (AIOKafkaProducer(bootstrap_servers=bootstrap_servers) if dlq_topic else None)
        )
        self._listener = TrackingRebalanceListener(self._consumer)

    def request_stop(self) -> None:
        self._stop_event.set()

    async def start(self) -> None:
        if self._dlq_producer is not None:
            await self._dlq_producer.start()
        await self._consumer.start()
        if hasattr(self._consumer, "subscribe"):
            self._consumer.subscribe(self.topics, listener=self._listener)

    async def stop(self) -> None:
        try:
            await self._consumer.stop()
        finally:
            if self._dlq_producer is not None:
                await self._dlq_producer.stop()

    async def _send_to_dlq(self, record: Any, exc: Exception) -> None:
        raw_text = record.value.decode("utf-8", errors="replace") if record.value else "{}"
        event_id = None
        source = None
        raw_payload: Any = {"raw": raw_text}
        try:
            parsed = json.loads(raw_text)
            if isinstance(parsed, dict):
                event_id = parsed.get("event_id")
                source = parsed.get("source")
                raw_payload = parsed
        except json.JSONDecodeError:
            pass

        dlq_message = {
            "event_id": event_id,
            "source": source,
            "topic": record.topic,
            "error": str(exc),
            "raw_payload": raw_payload,
            "failed_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        }
        key = event_id.encode("utf-8") if isinstance(event_id, str) else None
        await self._dlq_producer.send_and_wait(
            self.dlq_topic,
            value=json.dumps(dlq_message).encode("utf-8"),
            key=key,
        )

    async def process_record(self, record: Any) -> None:
        tp = TopicPartition(record.topic, record.partition)
        next_offset = record.offset + 1
        try:
            await self._handler(record)
        except Exception as exc:
            logger.error(
                "consumer_record_failed",
                topic=record.topic,
                partition=record.partition,
                offset=record.offset,
                error=str(exc),
            )
            if self.dlq_topic and self._dlq_producer is not None:
                await self._send_to_dlq(record, exc)
                await self._consumer.commit({tp: next_offset})
                self._listener.pending_commits[tp] = next_offset
                return
            raise

        await self._consumer.commit({tp: next_offset})
        self._listener.pending_commits[tp] = next_offset

    async def poll_once(self, timeout_ms: int = 1000) -> int:
        batches = await self._consumer.getmany(timeout_ms=timeout_ms)
        processed = 0
        for records in batches.values():
            for record in records:
                await self.process_record(record)
                processed += 1
        return processed

    async def run_forever(self) -> None:
        await self.start()
        try:
            while not self._stop_event.is_set():
                await self.poll_once(timeout_ms=1000)
        finally:
            await self.stop()


def install_signal_handlers(consumer: BaseKafkaConsumer) -> None:
    loop = asyncio.get_running_loop()
    for sig_name in ("SIGINT", "SIGTERM"):
        sig = getattr(signal, sig_name, None)
        if sig is None:
            continue
        try:
            loop.add_signal_handler(sig, consumer.request_stop)
        except NotImplementedError:
            signal.signal(sig, lambda *_: consumer.request_stop())

