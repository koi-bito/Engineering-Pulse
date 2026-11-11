from typing import Any

import structlog
from elasticsearch import AsyncElasticsearch, BadRequestError

logger = structlog.get_logger(__name__)

ES_ALIAS = "engineering-events"
ES_INITIAL_INDEX = "engineering-events-v1"

INDEX_MAPPING: dict[str, Any] = {
    "properties": {
        "title": {"type": "text"},
        "summary": {"type": "text"},
        "message": {"type": "text"},
        "author": {"type": "keyword"},
        "repo": {"type": "keyword"},
        "type": {"type": "keyword"},
        "source": {"type": "keyword"},
        "timestamp": {"type": "date"},
    }
}


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
            await self._client.indices.create(
                index=ES_INITIAL_INDEX,
                mappings=INDEX_MAPPING,
                aliases={ES_ALIAS: {}},
            )
            logger.info("es_index_created", index=ES_INITIAL_INDEX, alias=ES_ALIAS)
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

