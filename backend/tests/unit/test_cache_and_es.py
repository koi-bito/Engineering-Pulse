from app.services.cache import (
    AVG_CYCLE_TIME_7D_KEY,
    METRICS_CACHE_TTL_SECONDS,
    PRS_MERGED_TODAY_KEY,
    SPRINT_CACHE_TTL_SECONDS,
    get_cached_json,
    invalidate_metrics_cache,
    set_cached_json,
    sprint_active_key,
)
from app.services.es_index import ES_ALIAS, ES_INITIAL_INDEX, ElasticsearchIndexer
from tests.conftest import FakeAsyncRedis


class FakeESIndices:
    def __init__(self, alias_exists: bool = False) -> None:
        self.alias_exists = alias_exists
        self.created: list[dict] = []

    async def exists_alias(self, *, name: str) -> bool:
        return self.alias_exists and name == ES_ALIAS

    async def create(self, **kwargs) -> None:
        self.created.append(kwargs)
        self.alias_exists = True


class FakeESClient:
    def __init__(self, alias_exists: bool = False) -> None:
        self.indices = FakeESIndices(alias_exists=alias_exists)
        self.indexed: list[dict] = []

    async def index(self, **kwargs) -> None:
        self.indexed.append(kwargs)

    async def close(self) -> None:
        pass


async def test_metrics_and_sprint_cache_helpers() -> None:
    redis = FakeAsyncRedis()
    await set_cached_json(
        redis, PRS_MERGED_TODAY_KEY, {"count": 12}, METRICS_CACHE_TTL_SECONDS
    )
    await set_cached_json(
        redis, sprint_active_key("ENG"), {"open": 4}, SPRINT_CACHE_TTL_SECONDS
    )

    assert await get_cached_json(redis, PRS_MERGED_TODAY_KEY) == {"count": 12}
    assert redis.store[PRS_MERGED_TODAY_KEY][1] == 300
    assert redis.store[sprint_active_key("ENG")][1] == 60

    await invalidate_metrics_cache(redis, project_key="ENG")
    assert await get_cached_json(redis, PRS_MERGED_TODAY_KEY) is None
    assert await get_cached_json(redis, AVG_CYCLE_TIME_7D_KEY) is None
    assert await get_cached_json(redis, sprint_active_key("ENG")) is None


async def test_es_indexer_uses_alias_and_respects_enabled_flag() -> None:
    fake_es = FakeESClient()
    indexer = ElasticsearchIndexer("http://localhost:9200", enabled=True, client=fake_es)
    await indexer.ensure_index()
    await indexer.ensure_index()  # Idempotent when alias already exists.
    await indexer.index_event("pr:1", {"title": "Fix bug"})

    assert len(fake_es.indices.created) == 1
    assert fake_es.indices.created[0]["index"] == ES_INITIAL_INDEX
    assert fake_es.indices.created[0]["aliases"] == {ES_ALIAS: {}}
    assert fake_es.indexed == [
        {"index": ES_ALIAS, "id": "pr:1", "document": {"title": "Fix bug"}}
    ]

    disabled_es = FakeESClient()
    disabled_indexer = ElasticsearchIndexer(
        "http://localhost:9200", enabled=False, client=disabled_es
    )
    await disabled_indexer.ensure_index()
    await disabled_indexer.index_event("pr:2", {"title": "Ignored"})
    assert disabled_es.indices.created == []
    assert disabled_es.indexed == []

