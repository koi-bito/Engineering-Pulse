"""create metrics_snapshots, alert_configs, and workflow_alerts tables

Revision ID: 0004_anomaly_detection_schema
Revises: 0003_multi_tenant_schema
Create Date: 2026-11-17
"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = "0004_anomaly_detection_schema"
down_revision: str = "0003_multi_tenant_schema"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # 1. Create metrics_snapshots table
    op.create_table(
        "metrics_snapshots",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "team_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("teams.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("metric_name", sa.String(100), nullable=False),
        sa.Column("value", sa.Float(), nullable=False),
        sa.Column(
            "recorded_at",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.text("NOW()"),
            nullable=False,
        ),
    )

    op.create_index(
        "idx_metrics_snapshots_lookup",
        "metrics_snapshots",
        ["team_id", "metric_name", "recorded_at"],
    )

    # Unique hourly constraint to guarantee idempotent snapshot writing
    op.execute(
        """
        CREATE UNIQUE INDEX uq_metrics_snapshots_hourly
        ON metrics_snapshots (team_id, metric_name, (date_trunc('hour', recorded_at AT TIME ZONE 'UTC')));
        """
    )

    # 2. Create alert_configs table
    op.create_table(
        "alert_configs",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "team_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("teams.id", ondelete="CASCADE"),
            nullable=True,
        ),
        sa.Column("metric_name", sa.String(100), nullable=False),
        sa.Column("std_multiplier", sa.Float(), server_default="2.0", nullable=False),
        sa.Column("min_data_points", sa.Integer(), server_default="7", nullable=False),
        sa.Column(
            "suppression_window_minutes",
            sa.Integer(),
            server_default="60",
            nullable=False,
        ),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.text("NOW()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.text("NOW()"),
            nullable=False,
        ),
    )

    op.execute(
        """
        CREATE UNIQUE INDEX uq_alert_configs_team_metric
        ON alert_configs (COALESCE(team_id, '00000000-0000-0000-0000-000000000000'::uuid), metric_name);
        """
    )

    # 3. Create workflow_alerts table
    op.create_table(
        "workflow_alerts",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "team_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("teams.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("metric_name", sa.String(100), nullable=False),
        sa.Column("actual_value", sa.Float(), nullable=False),
        sa.Column("expected_value", sa.Float(), nullable=False),
        sa.Column("severity", sa.String(50), server_default="warning", nullable=False),
        sa.Column("status", sa.String(50), server_default="active", nullable=False),
        sa.Column("group_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column(
            "details",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=True,
        ),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.text("NOW()"),
            nullable=False,
        ),
        sa.Column("resolved_at", sa.TIMESTAMP(timezone=True), nullable=True),
    )

    op.create_index(
        "idx_workflow_alerts_status",
        "workflow_alerts",
        ["team_id", "status", "created_at"],
    )
    op.create_index(
        "idx_workflow_alerts_metric",
        "workflow_alerts",
        ["team_id", "metric_name", "status"],
    )


def downgrade() -> None:
    op.drop_index("idx_workflow_alerts_metric", table_name="workflow_alerts")
    op.drop_index("idx_workflow_alerts_status", table_name="workflow_alerts")
    op.drop_table("workflow_alerts")

    op.execute("DROP INDEX IF EXISTS uq_alert_configs_team_metric;")
    op.drop_table("alert_configs")

    op.execute("DROP INDEX IF EXISTS uq_metrics_snapshots_hourly;")
    op.drop_index("idx_metrics_snapshots_lookup", table_name="metrics_snapshots")
    op.drop_table("metrics_snapshots")
