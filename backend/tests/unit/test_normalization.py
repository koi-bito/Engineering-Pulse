from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from app.schemas.events import (
    PullRequestEvent,
    normalize_github_commits,
    normalize_github_pr,
    normalize_jira_ticket,
    normalize_jira_transitions,
)


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


def test_normalize_github_commits() -> None:
    payload = {
        "repository": {"full_name": "acme/api"},
        "commits": [
            {
                "id": "abc123",
                "message": "feat: add consumer",
                "timestamp": "2026-11-11T15:30:00+05:30",
                "author": {"username": "kunal"},
            }
        ],
    }

    commits = normalize_github_commits(payload)
    assert len(commits) == 1
    assert commits[0].sha == "abc123"
    assert commits[0].author == "kunal"
    assert commits[0].timestamp == datetime(2026, 11, 11, 10, 0, tzinfo=UTC)


def test_normalize_jira_ticket_and_transitions_convert_to_utc() -> None:
    payload = {
        "webhookEvent": "jira:issue_updated",
        "timestamp": 1794391200000,
        "issue": {
            "key": "ENG-42",
            "fields": {
                "project": {"key": "ENG"},
                "summary": "Fix consumer lag",
                "status": {"name": "Done"},
                "assignee": {"displayName": "kunal"},
                "created": "2026-11-11T13:30:00+05:30",
                "resolutiondate": "2026-11-11T16:30:00+05:30",
            },
        },
        "changelog": {
            "items": [
                {"field": "status", "fromString": "In Progress", "toString": "Done"}
            ]
        },
    }

    ticket = normalize_jira_ticket(payload)
    transitions = normalize_jira_transitions(payload)

    assert ticket.id == "ENG-42"
    assert ticket.created_at == datetime(2026, 11, 11, 8, 0, tzinfo=UTC)
    assert ticket.resolved_at == datetime(2026, 11, 11, 11, 0, tzinfo=UTC)
    assert ticket.cycle_time_hours == 3.0
    assert len(transitions) == 1
    assert transitions[0].from_status == "In Progress"
    assert transitions[0].to_status == "Done"
    assert transitions[0].transitioned_at.tzinfo == UTC
