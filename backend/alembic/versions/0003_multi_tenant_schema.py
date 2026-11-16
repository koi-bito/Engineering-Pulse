"""create multi-tenant tables and add tenant columns

Revision ID: 0003_multi_tenant_schema
Revises: 0002_create_users_table
Create Date: 2026-11-17
"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = "0003_multi_tenant_schema"
down_revision: str = "0002_create_users_table"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # 1. Create orgs table
    op.create_table(
        "orgs",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.text("NOW()"),
            nullable=False,
        ),
    )

    # 2. Create teams table
    op.create_table(
        "teams",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "org_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("orgs.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.text("NOW()"),
            nullable=False,
        ),
    )
    op.create_index("ix_teams_org_id", "teams", ["org_id"])

    # 3. Create team_members table
    op.create_table(
        "team_members",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "team_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("teams.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "user_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("users.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("role", sa.String(50), server_default="member", nullable=False),
        sa.Column(
            "joined_at",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.text("NOW()"),
            nullable=False,
        ),
        sa.UniqueConstraint("team_id", "user_id", name="uq_team_members_team_user"),
    )
    op.create_index("ix_team_members_team_id", "team_members", ["team_id"])
    op.create_index("ix_team_members_user_id", "team_members", ["user_id"])

    # 4. Create webhook_registrations table
    op.create_table(
        "webhook_registrations",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "org_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("orgs.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "team_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("teams.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("source", sa.String(50), nullable=False),
        sa.Column("secret", sa.String(255), nullable=False, unique=True),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.text("NOW()"),
            nullable=False,
        ),
    )
    op.create_index("ix_webhook_registrations_secret", "webhook_registrations", ["secret"])
    op.create_index("ix_webhook_registrations_org_team", "webhook_registrations", ["org_id", "team_id"])

    # 5. Add tenant columns & composite indexes to pull_requests
    op.add_column("pull_requests", sa.Column("org_id", postgresql.UUID(as_uuid=True), nullable=True))
    op.add_column("pull_requests", sa.Column("team_id", postgresql.UUID(as_uuid=True), nullable=True))
    op.create_index("ix_pull_requests_team_opened_at", "pull_requests", ["team_id", "opened_at"])
    op.create_index("ix_pull_requests_org_team", "pull_requests", ["org_id", "team_id"])

    # 6. Add tenant columns & composite indexes to commits
    op.add_column("commits", sa.Column("org_id", postgresql.UUID(as_uuid=True), nullable=True))
    op.add_column("commits", sa.Column("team_id", postgresql.UUID(as_uuid=True), nullable=True))
    op.create_index("ix_commits_team_timestamp", "commits", ["team_id", "timestamp"])
    op.create_index("ix_commits_org_team", "commits", ["org_id", "team_id"])

    # 7. Add tenant columns & composite indexes to jira_tickets
    op.add_column("jira_tickets", sa.Column("org_id", postgresql.UUID(as_uuid=True), nullable=True))
    op.add_column("jira_tickets", sa.Column("team_id", postgresql.UUID(as_uuid=True), nullable=True))
    op.create_index("ix_jira_tickets_team_created_at", "jira_tickets", ["team_id", "created_at"])
    op.create_index("ix_jira_tickets_org_team", "jira_tickets", ["org_id", "team_id"])

    # 8. Add tenant columns & composite indexes to jira_transitions
    op.add_column("jira_transitions", sa.Column("org_id", postgresql.UUID(as_uuid=True), nullable=True))
    op.add_column("jira_transitions", sa.Column("team_id", postgresql.UUID(as_uuid=True), nullable=True))
    op.create_index("ix_jira_transitions_team_id", "jira_transitions", ["team_id"])


def downgrade() -> None:
    # Drop columns and indexes from event tables
    op.drop_index("ix_jira_transitions_team_id", table_name="jira_transitions")
    op.drop_column("jira_transitions", "team_id")
    op.drop_column("jira_transitions", "org_id")

    op.drop_index("ix_jira_tickets_org_team", table_name="jira_tickets")
    op.drop_index("ix_jira_tickets_team_created_at", table_name="jira_tickets")
    op.drop_column("jira_tickets", "team_id")
    op.drop_column("jira_tickets", "org_id")

    op.drop_index("ix_commits_org_team", table_name="commits")
    op.drop_index("ix_commits_team_timestamp", table_name="commits")
    op.drop_column("commits", "team_id")
    op.drop_column("commits", "org_id")

    op.drop_index("ix_pull_requests_org_team", table_name="pull_requests")
    op.drop_index("ix_pull_requests_team_opened_at", table_name="pull_requests")
    op.drop_column("pull_requests", "team_id")
    op.drop_column("pull_requests", "org_id")

    # Drop tenant tables
    op.drop_table("webhook_registrations")
    op.drop_table("team_members")
    op.drop_table("teams")
    op.drop_table("orgs")
