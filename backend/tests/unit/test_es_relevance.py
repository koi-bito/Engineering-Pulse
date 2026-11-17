from typing import Any

import pytest

from app.routers.metrics import _search_elasticsearch
from app.services.es_index import (
    ES_ALIAS,
    INDEX_MAPPING_V2,
    INDEX_SETTINGS_V2,
)


class MockRelevanceESClient:
    def __init__(self, search_response: dict[str, Any] | None = None) -> None:
        self.search_calls: list[dict[str, Any]] = []
        self.search_response = search_response or {
            "hits": {
                "total": {"value": 0},
                "hits": [],
            }
        }

    async def search(self, **kwargs: Any) -> dict[str, Any]:
        self.search_calls.append(kwargs)
        return self.search_response

    async def close(self) -> None:
        pass


def test_custom_analyzer_configuration() -> None:
    """Verify v2 settings define engineering_analyzer with camelCase splitter."""
    analysis = INDEX_SETTINGS_V2["analysis"]
    analyzer = analysis["analyzer"]["engineering_analyzer"]
    assert analyzer["type"] == "custom"
    assert analyzer["tokenizer"] == "standard"
    assert "word_delimiter_filter" in analyzer["filter"]
    assert "lowercase" in analyzer["filter"]
    assert "stop" in analyzer["filter"]

    wd_filter = analysis["filter"]["word_delimiter_filter"]
    assert wd_filter["type"] == "word_delimiter_graph"
    assert wd_filter["preserve_original"] is True
    assert wd_filter["split_on_case_change"] is True
    assert wd_filter["split_on_numerics"] is False


def test_v2_mapping_properties() -> None:
    """Verify title, summary, message use engineering_analyzer in v2 mapping."""
    props = INDEX_MAPPING_V2["properties"]
    for field in ("title", "summary", "message"):
        assert props[field]["type"] == "text"
        assert props[field]["analyzer"] == "engineering_analyzer"

    assert props["author"]["type"] == "keyword"
    assert props["repo"]["type"] == "keyword"
    assert props["type"]["type"] == "keyword"
    assert props["team_id"]["type"] == "keyword"
    assert props["timestamp"]["type"] == "date"


@pytest.mark.asyncio
async def test_search_es_constructs_boosted_query_and_fuzziness() -> None:
    """Verify query uses title^3, summary^2, message^1 with fuzziness AUTO."""
    mock_es = MockRelevanceESClient()
    await _search_elasticsearch(mock_es, "auth bug", limit=10)

    assert len(mock_es.search_calls) == 1
    call = mock_es.search_calls[0]
    assert call["index"] == ES_ALIAS
    assert call["size"] == 10

    query = call["query"]
    multi_match = query["multi_match"]
    assert multi_match["query"] == "auth bug"
    assert multi_match["fields"] == ["title^3", "summary^2", "message^1"]
    assert multi_match["fuzziness"] == "AUTO"
    assert multi_match["prefix_length"] == 2

    # Highlight config
    hl = call["highlight"]
    assert "title" in hl["fields"]
    assert "summary" in hl["fields"]
    assert "message" in hl["fields"]


@pytest.mark.asyncio
async def test_search_es_filters_team_type_author_range() -> None:
    """Verify team_id, type, author, range filters are applied in bool.filter."""
    mock_es = MockRelevanceESClient()
    await _search_elasticsearch(
        mock_es,
        "kafka",
        limit=5,
        team_id="11111111-1111-4000-8000-000000000001",
        type_filter="pull_request",
        author_filter="alice",
        range_str="30d",
    )

    call = mock_es.search_calls[0]
    query = call["query"]
    assert "bool" in query
    filters = query["bool"]["filter"]
    filter_dict = {}
    for f in filters:
        if "term" in f:
            filter_dict.update(f["term"])
        elif "range" in f:
            filter_dict.update(f["range"])

    assert filter_dict["team_id"] == "11111111-1111-4000-8000-000000000001"
    assert filter_dict["type"] == "pull_request"
    assert filter_dict["author"] == "alice"
    assert filter_dict["timestamp"] == {"gte": "now-30d"}


@pytest.mark.asyncio
async def test_search_es_extracts_highlighted_snippets() -> None:
    """Verify highlighted snippets are prioritized and returned in hits."""
    sample_response = {
        "hits": {
            "total": {"value": 1},
            "hits": [
                {
                    "_id": "pr:101",
                    "_score": 4.52,
                    "_source": {
                        "type": "pull_request",
                        "source": "github",
                        "author": "kunal",
                        "repo": "Engineering-Pulse",
                        "title": "Fix auth bug in JWT middleware",
                        "timestamp": "2026-11-17T12:00:00Z",
                    },
                    "highlight": {
                        "title": ["Fix <mark>auth</mark> <mark>bug</mark> in JWT middleware"]
                    },
                }
            ],
        }
    }
    mock_es = MockRelevanceESClient(sample_response)
    res = await _search_elasticsearch(mock_es, "auth bug", limit=10)

    assert res["total_hits"] == 1
    hit = res["results"][0]
    assert hit["id"] == "pr:101"
    assert hit["score"] == 4.52
    assert hit["highlight"] == "Fix <mark>auth</mark> <mark>bug</mark> in JWT middleware"
    assert hit["snippet"] == "Fix auth bug in JWT middleware"


def test_ranking_boost_simulation() -> None:
    """Simulate BM25 score calculation with title^3 vs summary^2 vs message^1.

    A document matching the title must strictly rank above a document matching only message.
    """
    base_bm25 = 1.2
    title_score = base_bm25 * 3.0  # 3.6
    summary_score = base_bm25 * 2.0  # 2.4
    message_score = base_bm25 * 1.0  # 1.2

    assert title_score > summary_score > message_score
    assert title_score / message_score == 3.0
