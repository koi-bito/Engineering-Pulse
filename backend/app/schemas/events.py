from datetime import UTC, datetime
from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator, model_validator


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


class WebhookEnvelope(BaseModel):
    """Canonical Kafka message wrapper produced by webhook ingestion."""

    event_id: str
    source: Literal["github", "jira"]
    type: str
    received_at: datetime
    payload: dict[str, Any]

    @field_validator("received_at")
    @classmethod
    def normalize_received_at(cls, value: datetime) -> datetime:
        return _ensure_utc(value)


class PullRequestEvent(BaseModel):
    id: int | str
    repo: str = Field(min_length=1)
    author: str = Field(min_length=1)
    title: str = Field(min_length=1)
    state: str = Field(min_length=1)
    opened_at: datetime
    merged_at: datetime | None = None
    cycle_time_hours: float | None = None

    @field_validator("opened_at", "merged_at")
    @classmethod
    def normalize_timestamps(cls, value: datetime | None) -> datetime | None:
        return _ensure_utc(value) if value is not None else None

    @model_validator(mode="after")
    def compute_cycle_time(self) -> "PullRequestEvent":
        if self.merged_at is not None:
            if self.merged_at < self.opened_at:
                raise ValueError("merged_at cannot be earlier than opened_at")
            if self.cycle_time_hours is None:
                delta_seconds = (self.merged_at - self.opened_at).total_seconds()
                self.cycle_time_hours = round(delta_seconds / 3600, 2)
        return self


class CommitEvent(BaseModel):
    id: str = Field(min_length=1)
    repo: str = Field(min_length=1)
    author: str = Field(min_length=1)
    message: str
    sha: str = Field(min_length=1)
    timestamp: datetime

    @field_validator("timestamp")
    @classmethod
    def normalize_timestamp(cls, value: datetime) -> datetime:
        return _ensure_utc(value)


class JiraTicketEvent(BaseModel):
    id: str = Field(min_length=1)
    project: str = Field(min_length=1)
    assignee: str | None = None
    summary: str = Field(min_length=1)
    status: str = Field(min_length=1)
    created_at: datetime
    resolved_at: datetime | None = None
    cycle_time_hours: float | None = None

    @field_validator("created_at", "resolved_at")
    @classmethod
    def normalize_timestamps(cls, value: datetime | None) -> datetime | None:
        return _ensure_utc(value) if value is not None else None

    @model_validator(mode="after")
    def compute_cycle_time(self) -> "JiraTicketEvent":
        if self.resolved_at is not None:
            if self.resolved_at < self.created_at:
                raise ValueError("resolved_at cannot be earlier than created_at")
            if self.cycle_time_hours is None:
                delta_seconds = (self.resolved_at - self.created_at).total_seconds()
                self.cycle_time_hours = round(delta_seconds / 3600, 2)
        return self


class JiraTransitionEvent(BaseModel):
    ticket_id: str = Field(min_length=1)
    from_status: str = Field(min_length=1)
    to_status: str = Field(min_length=1)
    transitioned_at: datetime

    @field_validator("transitioned_at")
    @classmethod
    def normalize_timestamp(cls, value: datetime) -> datetime:
        return _ensure_utc(value)


class _GitHubUser(BaseModel):
    login: str = Field(min_length=1)


class _GitHubPRBody(BaseModel):
    id: int | str
    title: str = Field(min_length=1)
    state: str = Field(min_length=1)
    user: _GitHubUser
    created_at: datetime
    merged_at: datetime | None = None


class _GitHubRepository(BaseModel):
    full_name: str = Field(min_length=1)


class _GitHubPRWebhookPayload(BaseModel):
    pull_request: _GitHubPRBody
    repository: _GitHubRepository


def normalize_github_pr(payload: dict[str, Any]) -> PullRequestEvent:
    """Validate and convert a raw GitHub pull_request webhook payload."""
    parsed = _GitHubPRWebhookPayload.model_validate(payload)
    pr = parsed.pull_request
    return PullRequestEvent(
        id=pr.id,
        repo=parsed.repository.full_name,
        author=pr.user.login,
        title=pr.title,
        state=pr.state,
        opened_at=pr.created_at,
        merged_at=pr.merged_at,
    )

