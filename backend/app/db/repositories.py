import json
from datetime import datetime
from typing import Any

import asyncpg

from app.schemas.events import (
    CommitEvent,
    JiraTicketEvent,
    JiraTransitionEvent,
    PullRequestEvent,
)


async def upsert_pull_request(conn: asyncpg.Connection, pr: PullRequestEvent) -> None:
    await conn.execute(
        """
        INSERT INTO pull_requests (
            id, repo, author, title, state, opened_at, merged_at, cycle_time_hours
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        ON CONFLICT (id) DO UPDATE SET
            repo = EXCLUDED.repo,
            author = EXCLUDED.author,
            title = EXCLUDED.title,
            state = EXCLUDED.state,
            opened_at = EXCLUDED.opened_at,
            merged_at = COALESCE(EXCLUDED.merged_at, pull_requests.merged_at),
            cycle_time_hours = COALESCE(
                EXCLUDED.cycle_time_hours, pull_requests.cycle_time_hours
            ),
            updated_at = NOW()
        """,
        str(pr.id),
        pr.repo,
        pr.author,
        pr.title,
        pr.state,
        pr.opened_at,
        pr.merged_at,
        pr.cycle_time_hours,
    )


async def upsert_commit(conn: asyncpg.Connection, commit: CommitEvent) -> None:
    await conn.execute(
        """
        INSERT INTO commits (id, repo, author, message, sha, timestamp)
        VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT (sha) DO NOTHING
        """,
        commit.id,
        commit.repo,
        commit.author,
        commit.message,
        commit.sha,
        commit.timestamp,
    )


async def upsert_jira_ticket(conn: asyncpg.Connection, ticket: JiraTicketEvent) -> None:
    await conn.execute(
        """
        INSERT INTO jira_tickets (
            id, project, assignee, summary, status, created_at, resolved_at, cycle_time_hours
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        ON CONFLICT (id) DO UPDATE SET
            project = EXCLUDED.project,
            assignee = EXCLUDED.assignee,
            summary = EXCLUDED.summary,
            status = EXCLUDED.status,
            created_at = EXCLUDED.created_at,
            resolved_at = COALESCE(EXCLUDED.resolved_at, jira_tickets.resolved_at),
            cycle_time_hours = COALESCE(
                EXCLUDED.cycle_time_hours, jira_tickets.cycle_time_hours
            ),
            updated_at = NOW()
        """,
        ticket.id,
        ticket.project,
        ticket.assignee,
        ticket.summary,
        ticket.status,
        ticket.created_at,
        ticket.resolved_at,
        ticket.cycle_time_hours,
    )


async def insert_jira_transition(
    conn: asyncpg.Connection, transition: JiraTransitionEvent
) -> None:
    await conn.execute(
        """
        INSERT INTO jira_transitions (ticket_id, from_status, to_status, transitioned_at)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (ticket_id, from_status, to_status, transitioned_at) DO NOTHING
        """,
        transition.ticket_id,
        transition.from_status,
        transition.to_status,
        transition.transitioned_at,
    )


async def insert_failed_event(
    conn: asyncpg.Connection,
    *,
    event_id: str | None,
    source: str | None,
    topic: str,
    error: str,
    raw_payload: dict[str, Any] | str,
    failed_at: datetime,
) -> None:
    payload_str = (
        raw_payload if isinstance(raw_payload, str) else json.dumps(raw_payload)
    )
    await conn.execute(
        """
        INSERT INTO failed_events (event_id, source, topic, error, raw_payload, failed_at)
        VALUES ($1, $2, $3, $4, $5::jsonb, $6)
        """,
        event_id,
        source,
        topic,
        error,
        payload_str,
        failed_at,
    )

