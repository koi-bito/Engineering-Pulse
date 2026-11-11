"""create initial workflow and dlq tables

Revision ID: 0001_initial_tables
Revises:
Create Date: 2026-11-11
"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = "0001_initial_tables"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "pull_requests",
        sa.Column("id", sa.Text(), primary_key=True),
        sa.Column("repo", sa.Text(), nullable=False),
        sa.Column("author", sa.Text(), nullable=False),
        sa.Column("title", sa.Text(), nullable=False),
        sa.Column("state", sa.Text(), nullable=False),
        sa.Column("opened_at", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("merged_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("cycle_time_hours", sa.Float(), nullable=True),
        sa.Column(
            "updated_at",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.text("NOW()"),
            nullable=False,
        ),
    )
    op.create_index("ix_pull_requests_repo", "pull_requests", ["repo"])
    op.create_index("ix_pull_requests_author", "pull_requests", ["author"])
    op.create_index("ix_pull_requests_opened_at", "pull_requests", ["opened_at"])
    op.create_index("ix_pull_requests_merged_at", "pull_requests", ["merged_at"])
    op.create_index("ix_pull_requests_state", "pull_requests", ["state"])

    op.create_table(
        "commits",
        sa.Column("id", sa.Text(), primary_key=True),
        sa.Column("repo", sa.Text(), nullable=False),
        sa.Column("author", sa.Text(), nullable=False),
        sa.Column("message", sa.Text(), nullable=False),
        sa.Column("sha", sa.Text(), nullable=False, unique=True),
        sa.Column("timestamp", sa.TIMESTAMP(timezone=True), nullable=False),
    )
    op.create_index("ix_commits_repo", "commits", ["repo"])
    op.create_index("ix_commits_author", "commits", ["author"])
    op.create_index("ix_commits_timestamp", "commits", ["timestamp"])

    op.create_table(
        "jira_tickets",
        sa.Column("id", sa.Text(), primary_key=True),
        sa.Column("project", sa.Text(), nullable=False),
        sa.Column("assignee", sa.Text(), nullable=True),
        sa.Column("summary", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("resolved_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("cycle_time_hours", sa.Float(), nullable=True),
        sa.Column(
            "updated_at",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.text("NOW()"),
            nullable=False,
        ),
    )
    op.create_index("ix_jira_tickets_project", "jira_tickets", ["project"])
    op.create_index("ix_jira_tickets_status", "jira_tickets", ["status"])
    op.create_index("ix_jira_tickets_created_at", "jira_tickets", ["created_at"])

    op.create_table(
        "jira_transitions",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("ticket_id", sa.Text(), nullable=False),
        sa.Column("from_status", sa.Text(), nullable=False),
        sa.Column("to_status", sa.Text(), nullable=False),
        sa.Column("transitioned_at", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.UniqueConstraint(
            "ticket_id",
            "from_status",
            "to_status",
            "transitioned_at",
            name="uq_jira_transitions_event",
        ),
    )
    op.create_index("ix_jira_transitions_ticket_id", "jira_transitions", ["ticket_id"])
    op.create_index(
        "ix_jira_transitions_transitioned_at",
        "jira_transitions",
        ["transitioned_at"],
    )

    op.create_table(
        "failed_events",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("event_id", sa.Text(), nullable=True),
        sa.Column("source", sa.Text(), nullable=True),
        sa.Column("topic", sa.Text(), nullable=False),
        sa.Column("error", sa.Text(), nullable=False),
        sa.Column("raw_payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column(
            "failed_at",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.text("NOW()"),
            nullable=False,
        ),
    )
    op.create_index("ix_failed_events_topic", "failed_events", ["topic"])
    op.create_index("ix_failed_events_failed_at", "failed_events", ["failed_at"])


def downgrade() -> None:
    op.drop_index("ix_failed_events_failed_at", table_name="failed_events")
    op.drop_index("ix_failed_events_topic", table_name="failed_events")
    op.drop_table("failed_events")

    op.drop_index("ix_jira_transitions_transitioned_at", table_name="jira_transitions")
    op.drop_index("ix_jira_transitions_ticket_id", table_name="jira_transitions")
    op.drop_table("jira_transitions")

    op.drop_index("ix_jira_tickets_created_at", table_name="jira_tickets")
    op.drop_index("ix_jira_tickets_status", table_name="jira_tickets")
    op.drop_index("ix_jira_tickets_project", table_name="jira_tickets")
    op.drop_table("jira_tickets")

    op.drop_index("ix_commits_timestamp", table_name="commits")
    op.drop_index("ix_commits_author", table_name="commits")
    op.drop_index("ix_commits_repo", table_name="commits")
    op.drop_table("commits")

    op.drop_index("ix_pull_requests_state", table_name="pull_requests")
    op.drop_index("ix_pull_requests_merged_at", table_name="pull_requests")
    op.drop_index("ix_pull_requests_opened_at", table_name="pull_requests")
    op.drop_index("ix_pull_requests_author", table_name="pull_requests")
    op.drop_index("ix_pull_requests_repo", table_name="pull_requests")
    op.drop_table("pull_requests")
