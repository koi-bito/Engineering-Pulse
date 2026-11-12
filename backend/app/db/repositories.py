import json
from datetime import UTC, datetime
from typing import Any

import asyncpg

from app.schemas.events import (
    CommitEvent,
    JiraTicketEvent,
    JiraTransitionEvent,
    PullRequestEvent,
)


def to_utc_iso(value: datetime | None) -> str | None:
    if value is None:
        return None
    utc_dt = value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
    return utc_dt.isoformat().replace("+00:00", "Z")


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


async def fetch_metrics_summary(conn: asyncpg.Connection) -> dict[str, Any]:
    row = await conn.fetchrow(
        """
        SELECT
            (
                SELECT COUNT(*)::int
                FROM pull_requests
                WHERE merged_at >= date_trunc('day', NOW() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC'
            ) AS prs_merged_today,
            (
                SELECT ROUND(AVG(cycle_time_hours)::numeric, 2)::float
                FROM pull_requests
                WHERE merged_at >= NOW() - INTERVAL '7 days'
                  AND cycle_time_hours IS NOT NULL
            ) AS avg_cycle_time_7d_hours,
            (
                SELECT ROUND(AVG(cycle_time_hours)::numeric, 2)::float
                FROM jira_tickets
                WHERE resolved_at >= NOW() - INTERVAL '7 days'
                  AND cycle_time_hours IS NOT NULL
            ) AS avg_ticket_cycle_time_7d_hours,
            (
                SELECT COUNT(*)::int
                FROM pull_requests
                WHERE state = 'open'
            ) AS open_prs,
            (
                SELECT COUNT(*)::int
                FROM jira_tickets
                WHERE resolved_at IS NULL
            ) AS active_tickets,
            (
                SELECT COUNT(*)::int
                FROM commits
                WHERE timestamp >= date_trunc('day', NOW() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC'
            ) AS commits_today
        """
    )
    return {
        "prs_merged_today": int(row["prs_merged_today"]) if row else 0,
        "avg_cycle_time_7d_hours": (
            float(row["avg_cycle_time_7d_hours"])
            if row and row["avg_cycle_time_7d_hours"] is not None
            else None
        ),
        "avg_ticket_cycle_time_7d_hours": (
            float(row["avg_ticket_cycle_time_7d_hours"])
            if row and row["avg_ticket_cycle_time_7d_hours"] is not None
            else None
        ),
        "open_prs": int(row["open_prs"]) if row else 0,
        "active_tickets": int(row["active_tickets"]) if row else 0,
        "commits_today": int(row["commits_today"]) if row else 0,
    }


async def fetch_pr_daily_counts(conn: asyncpg.Connection, days: int) -> list[dict[str, Any]]:
    rows = await conn.fetch(
        """
        WITH days AS (
            SELECT generate_series(
                (NOW() AT TIME ZONE 'UTC')::date - ($1::int - 1),
                (NOW() AT TIME ZONE 'UTC')::date,
                INTERVAL '1 day'
            )::date AS day
        ),
        merged AS (
            SELECT (merged_at AT TIME ZONE 'UTC')::date AS day, COUNT(*)::int AS merged_count
            FROM pull_requests
            WHERE merged_at >= ((NOW() AT TIME ZONE 'UTC')::date - ($1::int - 1))::timestamp AT TIME ZONE 'UTC'
            GROUP BY 1
        ),
        opened AS (
            SELECT (opened_at AT TIME ZONE 'UTC')::date AS day, COUNT(*)::int AS opened_count
            FROM pull_requests
            WHERE opened_at >= ((NOW() AT TIME ZONE 'UTC')::date - ($1::int - 1))::timestamp AT TIME ZONE 'UTC'
            GROUP BY 1
        )
        SELECT
            to_char(d.day, 'YYYY-MM-DD') AS date,
            COALESCE(m.merged_count, 0)::int AS merged_count,
            COALESCE(o.opened_count, 0)::int AS opened_count
        FROM days d
        LEFT JOIN merged m ON m.day = d.day
        LEFT JOIN opened o ON o.day = d.day
        ORDER BY d.day ASC
        """,
        days,
    )
    return [
        {
            "date": row["date"],
            "merged_count": int(row["merged_count"]),
            "opened_count": int(row["opened_count"]),
        }
        for row in rows
    ]


async def fetch_cycle_time_series(conn: asyncpg.Connection, days: int) -> list[dict[str, Any]]:
    rows = await conn.fetch(
        """
        WITH days AS (
            SELECT generate_series(
                (NOW() AT TIME ZONE 'UTC')::date - ($1::int - 1),
                (NOW() AT TIME ZONE 'UTC')::date,
                INTERVAL '1 day'
            )::date AS day
        ),
        ticket_daily AS (
            SELECT
                (resolved_at AT TIME ZONE 'UTC')::date AS day,
                AVG(cycle_time_hours) AS avg_ticket_hours,
                COUNT(*)::int AS resolved_tickets
            FROM jira_tickets
            WHERE resolved_at >= ((NOW() AT TIME ZONE 'UTC')::date - ($1::int - 1))::timestamp AT TIME ZONE 'UTC'
              AND cycle_time_hours IS NOT NULL
            GROUP BY 1
        ),
        pr_daily AS (
            SELECT
                (merged_at AT TIME ZONE 'UTC')::date AS day,
                AVG(cycle_time_hours) AS avg_pr_hours,
                COUNT(*)::int AS merged_prs
            FROM pull_requests
            WHERE merged_at >= ((NOW() AT TIME ZONE 'UTC')::date - ($1::int - 1))::timestamp AT TIME ZONE 'UTC'
              AND cycle_time_hours IS NOT NULL
            GROUP BY 1
        )
        SELECT
            to_char(d.day, 'YYYY-MM-DD') AS date,
            ROUND(t.avg_ticket_hours::numeric, 2)::float AS avg_ticket_cycle_time_hours,
            ROUND(
                AVG(t.avg_ticket_hours) OVER (
                    ORDER BY d.day ASC ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
                )::numeric,
                2
            )::float AS rolling_avg_hours,
            ROUND(p.avg_pr_hours::numeric, 2)::float AS avg_pr_cycle_time_hours,
            COALESCE(t.resolved_tickets, 0)::int AS resolved_tickets,
            COALESCE(p.merged_prs, 0)::int AS merged_prs
        FROM days d
        LEFT JOIN ticket_daily t ON t.day = d.day
        LEFT JOIN pr_daily p ON p.day = d.day
        ORDER BY d.day ASC
        """,
        days,
    )
    return [
        {
            "date": row["date"],
            "avg_ticket_cycle_time_hours": (
                float(row["avg_ticket_cycle_time_hours"])
                if row["avg_ticket_cycle_time_hours"] is not None
                else None
            ),
            "rolling_avg_hours": (
                float(row["rolling_avg_hours"])
                if row["rolling_avg_hours"] is not None
                else None
            ),
            "avg_pr_cycle_time_hours": (
                float(row["avg_pr_cycle_time_hours"])
                if row["avg_pr_cycle_time_hours"] is not None
                else None
            ),
            "resolved_tickets": int(row["resolved_tickets"]),
            "merged_prs": int(row["merged_prs"]),
        }
        for row in rows
    ]


async def fetch_active_sprint(
    conn: asyncpg.Connection,
    project_key: str | None = None,
) -> dict[str, Any]:
    if project_key:
        count_rows = await conn.fetch(
            """
            SELECT status, COUNT(*)::int AS count
            FROM jira_tickets
            WHERE project = $1
            GROUP BY status
            ORDER BY count DESC, status ASC
            """,
            project_key,
        )
        ticket_rows = await conn.fetch(
            """
            SELECT id, project, assignee, summary, status, created_at, resolved_at, cycle_time_hours
            FROM jira_tickets
            WHERE project = $1
            ORDER BY updated_at DESC, created_at DESC
            LIMIT 50
            """,
            project_key,
        )
    else:
        count_rows = await conn.fetch(
            """
            SELECT status, COUNT(*)::int AS count
            FROM jira_tickets
            GROUP BY status
            ORDER BY count DESC, status ASC
            """
        )
        ticket_rows = await conn.fetch(
            """
            SELECT id, project, assignee, summary, status, created_at, resolved_at, cycle_time_hours
            FROM jira_tickets
            ORDER BY updated_at DESC, created_at DESC
            LIMIT 50
            """
        )

    return {
        "project": project_key or "all",
        "status_counts": {row["status"]: int(row["count"]) for row in count_rows},
        "tickets": [
            {
                "id": row["id"],
                "project": row["project"],
                "assignee": row["assignee"],
                "summary": row["summary"],
                "status": row["status"],
                "created_at": to_utc_iso(row["created_at"]),
                "resolved_at": to_utc_iso(row["resolved_at"]),
                "cycle_time_hours": (
                    round(float(row["cycle_time_hours"]), 2)
                    if row["cycle_time_hours"] is not None
                    else None
                ),
            }
            for row in ticket_rows
        ],
    }


async def search_events_postgres(
    conn: asyncpg.Connection,
    query: str,
    limit: int = 20,
) -> list[dict[str, Any]]:
    escaped = query.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
    pattern = f"%{escaped}%"
    rows = await conn.fetch(
        """
        SELECT id, type, source, author, repo, timestamp, snippet
        FROM (
            SELECT
                'pr:' || id AS id,
                'pull_request' AS type,
                'github' AS source,
                author,
                repo,
                opened_at AS timestamp,
                title AS snippet
            FROM pull_requests
            WHERE title ILIKE $1 ESCAPE '\\' OR repo ILIKE $1 ESCAPE '\\' OR author ILIKE $1 ESCAPE '\\'
            UNION ALL
            SELECT
                'commit:' || sha AS id,
                'commit' AS type,
                'github' AS source,
                author,
                repo,
                timestamp,
                message AS snippet
            FROM commits
            WHERE message ILIKE $1 ESCAPE '\\' OR repo ILIKE $1 ESCAPE '\\' OR author ILIKE $1 ESCAPE '\\'
            UNION ALL
            SELECT
                'ticket:' || id AS id,
                'ticket' AS type,
                'jira' AS source,
                COALESCE(assignee, 'unassigned') AS author,
                project AS repo,
                created_at AS timestamp,
                summary AS snippet
            FROM jira_tickets
            WHERE summary ILIKE $1 ESCAPE '\\' OR project ILIKE $1 ESCAPE '\\' OR id ILIKE $1 ESCAPE '\\'
        ) AS combined
        ORDER BY timestamp DESC
        LIMIT $2
        """,
        pattern,
        limit,
    )
    return [
        {
            "id": row["id"],
            "type": row["type"],
            "source": row["source"],
            "author": row["author"],
            "repo": row["repo"],
            "timestamp": to_utc_iso(row["timestamp"]),
            "snippet": row["snippet"],
            "score": None,
        }
        for row in rows
    ]

