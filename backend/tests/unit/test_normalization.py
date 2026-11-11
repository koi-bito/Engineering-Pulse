from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from app.schemas.events import PullRequestEvent, normalize_github_pr


def test_normalize_valid_github_pr() -> None:
    payload = {
        "repository": {"full_name": "acme/engineering-pulse"},
        "pull_request": {
            "id": 42,
            "title": "Add Kafka webhook ingestion",
            "state": "closed",
            "user": {"login": "kunal"},
            "created_at": "2026-11-11T08:00:00Z",
            "merged_at": "2026-11-11T10:30:00Z",
        },
    }

    event = normalize_github_pr(payload)

    assert isinstance(event, PullRequestEvent)
    assert event.id == 42
    assert event.repo == "acme/engineering-pulse"
    assert event.author == "kunal"
    assert event.title == "Add Kafka webhook ingestion"
    assert event.state == "closed"
    assert event.opened_at == datetime(2026, 11, 11, 8, 0, tzinfo=UTC)
    assert event.merged_at == datetime(2026, 11, 11, 10, 30, tzinfo=UTC)
    assert event.cycle_time_hours == 2.5


def test_normalize_github_pr_missing_required_field_raises() -> None:
    payload = {
        "repository": {"full_name": "acme/engineering-pulse"},
        "pull_request": {
            "id": 42,
            "state": "open",
            "user": {"login": "kunal"},
            "created_at": "2026-11-11T08:00:00Z",
        },
    }

    with pytest.raises(ValidationError):
        normalize_github_pr(payload)


def test_pr_cycle_time_rejects_negative_duration() -> None:
    with pytest.raises(ValidationError):
        PullRequestEvent(
            id=1,
            repo="acme/repo",
            author="kunal",
            title="Fix bug",
            state="closed",
            opened_at=datetime(2026, 11, 11, 10, 0, tzinfo=UTC),
            merged_at=datetime(2026, 11, 11, 9, 0, tzinfo=UTC),
        )

