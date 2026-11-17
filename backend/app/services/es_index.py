from typing import Any

import structlog
from elasticsearch import AsyncElasticsearch, BadRequestError

logger = structlog.get_logger(__name__)

ES_ALIAS = "engineering-events"
ES_INITIAL_INDEX = "engineering-events-v1"
ES_V2_INDEX = "engineering-events-v2"

INDEX_SETTINGS_V2: dict[str, Any] = {
    "analysis": {
        "analyzer": {
            "engineering_analyzer": {
                "type": "custom",
                "tokenizer": "standard",
                "filter": [
                    "word_delimiter_filter",
                    "lowercase",
                    "stop",
                ],
            }
        },
        "filter": {
            "word_delimiter_filter": {
                "type": "word_delimiter_graph",
                "preserve_original": True,
                "split_on_case_change": True,
                "catenate_words": True,
                "catenate_all": True,
                "split_on_numerics": False,
                "stem_english_possessive": True,
            }
        },
    }
}

INDEX_MAPPING_V2: dict[str, Any] = {
    "properties": {
        "title": {"type": "text", "analyzer": "engineering_analyzer"},
        "summary": {"type": "text", "analyzer": "engineering_analyzer"},
        "message": {"type": "text", "analyzer": "engineering_analyzer"},
        "author": {"type": "keyword"},
        "repo": {"type": "keyword"},
        "type": {"type": "keyword"},
        "source": {"type": "keyword"},
        "org_id": {"type": "keyword"},
        "team_id": {"type": "keyword"},
        "timestamp": {"type": "date"},
    }
}

# Legacy v1 mapping preserved for backward compatibility and migration reference
INDEX_MAPPING_V1: dict[str, Any] = {
    "properties": {
        "title": {"type": "text"},
        "summary": {"type": "text"},
        "message": {"type": "text"},
        "author": {"type": "keyword"},
        "repo": {"type": "keyword"},
        "type": {"type": "keyword"},
        "source": {"type": "keyword"},
        "org_id": {"type": "keyword"},
        "team_id": {"type": "keyword"},
        "timestamp": {"type": "date"},
    }
}

INDEX_MAPPING = INDEX_MAPPING_V2


class ElasticsearchIndexer:
    """Secondary search indexer guarded by ES_ENABLED and alias engineering-events."""

    def __init__(
        self,
        url: str,
        enabled: bool = True,
        client: Any | None = None,
    ) -> None:
        self.enabled = enabled
        self._owns_client = client is None and enabled
        self._client = client if client is not None else (
            AsyncElasticsearch(url) if enabled else None
        )

    async def ensure_index(self) -> None:
        if not self.enabled or self._client is None:
            return
        if bool(await self._client.indices.exists_alias(name=ES_ALIAS)):
            return
        try:
            # Prefer v2 schema with custom engineering_analyzer
            target_index = ES_V2_INDEX
            await self._client.indices.create(
                index=target_index,
                settings=INDEX_SETTINGS_V2,
                mappings=INDEX_MAPPING_V2,
                aliases={ES_ALIAS: {}},
            )
            logger.info("es_index_created", index=target_index, alias=ES_ALIAS)
        except BadRequestError as exc:
            if "resource_already_exists_exception" not in str(exc):
                raise

    async def index_event(self, doc_id: str, document: dict[str, Any]) -> None:
        if not self.enabled or self._client is None:
            return
        await self._client.index(index=ES_ALIAS, id=doc_id, document=document)

    async def close(self) -> None:
        if self._owns_client and self._client is not None:
            await self._client.close()
