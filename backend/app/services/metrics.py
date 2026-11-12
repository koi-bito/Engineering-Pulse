from typing import Any

from aiokafka import AIOKafkaConsumer, TopicPartition
from aiokafka.admin import AIOKafkaAdminClient
from prometheus_client import Counter, Gauge

WEBHOOK_EVENTS_RECEIVED_TOTAL = Counter(
    "webhook_events_received_total",
    "Total webhook events received by source, type, and ingestion outcome",
    ["source", "event_type", "status"],
)

CACHE_HIT_RATIO = Gauge(
    "cache_hit_ratio",
    "Redis read-through cache hit ratio (hits / total lookups)",
)

KAFKA_CONSUMER_LAG = Gauge(
    "kafka_consumer_lag",
    "Kafka consumer group lag in messages",
    ["topic", "partition", "group"],
)

_MONITORED_TOPICS = ("github-events", "jira-events")

_cache_hits = 0
_cache_lookups = 0


def record_cache_lookup(hit: bool) -> float:
    """Update cumulative read-through cache hit ratio and return the current ratio."""
    global _cache_hits, _cache_lookups
    _cache_lookups += 1
    if hit:
        _cache_hits += 1
    ratio = _cache_hits / _cache_lookups
    CACHE_HIT_RATIO.set(ratio)
    return ratio


async def refresh_kafka_consumer_lag(
    bootstrap_servers: str,
    group_id: str = "engineering-pulse-consumers",
    *,
    admin_client: Any | None = None,
    consumer_client: Any | None = None,
) -> dict[TopicPartition, int]:
    """Measure per-partition Kafka consumer lag and update the Prometheus gauge."""
    owns_admin = admin_client is None
    owns_consumer = consumer_client is None
    admin = admin_client or AIOKafkaAdminClient(
        bootstrap_servers=bootstrap_servers, request_timeout_ms=5000
    )
    consumer = consumer_client or AIOKafkaConsumer(
        bootstrap_servers=bootstrap_servers,
        group_id=None,
        request_timeout_ms=5000,
    )

    lags: dict[TopicPartition, int] = {}
    admin_started = False
    consumer_started = False
    try:
        if owns_admin:
            await admin.start()
            admin_started = True
        if owns_consumer:
            await consumer.start()
            consumer_started = True

        committed = await admin.list_consumer_group_offsets(group_id)
        partitions = list(committed.keys())
        if not partitions:
            for topic in _MONITORED_TOPICS:
                topic_parts = consumer.partitions_for_topic(topic) or set()
                partitions.extend(TopicPartition(topic, p) for p in sorted(topic_parts))

        if not partitions:
            return lags

        end_offsets = await consumer.end_offsets(partitions)
        for tp in partitions:
            end_offset = end_offsets.get(tp, 0)
            committed_meta = committed.get(tp)
            committed_offset = (
                committed_meta.offset
                if committed_meta is not None and committed_meta.offset >= 0
                else 0
            )
            lag = max(end_offset - committed_offset, 0)
            lags[tp] = lag
            KAFKA_CONSUMER_LAG.labels(
                topic=tp.topic, partition=str(tp.partition), group=group_id
            ).set(lag)
        return lags
    finally:
        if consumer_started:
            await consumer.stop()
        if admin_started:
            await admin.close()
