from aiokafka import TopicPartition
from aiokafka.structs import OffsetAndMetadata

from app.services import metrics as metrics_service


class FakeAdminClient:
    def __init__(self, committed: dict[TopicPartition, OffsetAndMetadata]) -> None:
        self._committed = committed

    async def list_consumer_group_offsets(
        self, group_id: str
    ) -> dict[TopicPartition, OffsetAndMetadata]:
        return self._committed


class FakeOffsetConsumer:
    def __init__(
        self,
        end_offsets: dict[TopicPartition, int],
        partitions_by_topic: dict[str, set[int]] | None = None,
    ) -> None:
        self._end_offsets = end_offsets
        self._partitions_by_topic = partitions_by_topic or {}

    def partitions_for_topic(self, topic: str) -> set[int] | None:
        return self._partitions_by_topic.get(topic)

    async def end_offsets(self, partitions: list[TopicPartition]) -> dict[TopicPartition, int]:
        return {tp: self._end_offsets[tp] for tp in partitions if tp in self._end_offsets}


def test_record_cache_lookup_updates_hit_ratio(monkeypatch) -> None:
    monkeypatch.setattr(metrics_service, "_cache_hits", 0)
    monkeypatch.setattr(metrics_service, "_cache_lookups", 0)

    assert metrics_service.record_cache_lookup(False) == 0.0
    assert metrics_service.record_cache_lookup(True) == 0.5
    assert metrics_service.record_cache_lookup(True) == 2 / 3
    assert metrics_service.CACHE_HIT_RATIO._value.get() == 2 / 3


async def test_refresh_kafka_consumer_lag_updates_gauge() -> None:
    tp0 = TopicPartition("github-events", 0)
    tp1 = TopicPartition("github-events", 1)
    admin = FakeAdminClient(
        {
            tp0: OffsetAndMetadata(offset=7, metadata=""),
            tp1: OffsetAndMetadata(offset=10, metadata=""),
        }
    )
    consumer = FakeOffsetConsumer({tp0: 12, tp1: 10})

    lags = await metrics_service.refresh_kafka_consumer_lag(
        "localhost:9092",
        group_id="test-group",
        admin_client=admin,
        consumer_client=consumer,
    )

    assert lags == {tp0: 5, tp1: 0}
    assert (
        metrics_service.KAFKA_CONSUMER_LAG.labels(
            topic="github-events", partition="0", group="test-group"
        )._value.get()
        == 5.0
    )


async def test_refresh_kafka_consumer_lag_falls_back_to_topic_partitions() -> None:
    tp0 = TopicPartition("jira-events", 0)
    admin = FakeAdminClient({})
    consumer = FakeOffsetConsumer(
        {tp0: 4},
        partitions_by_topic={"github-events": set(), "jira-events": {0}},
    )

    lags = await metrics_service.refresh_kafka_consumer_lag(
        "localhost:9092",
        group_id="empty-group",
        admin_client=admin,
        consumer_client=consumer,
    )

    assert lags == {tp0: 4}

