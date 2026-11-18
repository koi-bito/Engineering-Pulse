import math
import uuid
from datetime import datetime
from typing import Any

import asyncpg
import structlog

from app.db.repositories import (
    auto_resolve_alerts_for_metric,
    create_workflow_alert,
    fetch_metrics_snapshots,
    fetch_recent_alerts_in_window,
    get_or_create_alert_config,
    group_recent_alerts,
    record_metrics_snapshot,
)
from app.services.pubsub import publish_workflow_alert

logger = structlog.get_logger(__name__)

TRACKED_METRICS = [
    "avg_cycle_time",
    "prs_merged_count",
    "tickets_opened_count",
    "tickets_resolved_count",
    "stalled_prs_count",
]


def calculate_rolling_stats(values: list[float]) -> tuple[float, float]:
    """Calculate mean and sample standard deviation of values."""
    if not values:
        return 0.0, 0.0
    n = len(values)
    mean = sum(values) / n
    if n < 2:
        return mean, 0.0
    variance = sum((x - mean) ** 2 for x in values) / (n - 1)
    std = math.sqrt(variance)
    return mean, std


async def compute_team_metrics_for_hour(
    conn: asyncpg.Connection,
    team_id: uuid.UUID,
) -> dict[str, float]:
    """Compute hourly snapshot metrics for a single team."""
    # 1. Average cycle time over last 7 days (PRs + Jira tickets)
    cycle_row = await conn.fetchrow(
        """
        SELECT
            COALESCE(
                (SELECT AVG(cycle_time_hours) FROM pull_requests WHERE team_id = $1 AND merged_at >= (NOW() AT TIME ZONE 'UTC') - INTERVAL '7 days' AND cycle_time_hours IS NOT NULL),
                (SELECT AVG(cycle_time_hours) FROM jira_tickets WHERE team_id = $1 AND resolved_at >= (NOW() AT TIME ZONE 'UTC') - INTERVAL '7 days' AND cycle_time_hours IS NOT NULL),
                0.0
            )::float AS avg_cycle_time
        """,
        team_id,
    )
    avg_cycle_time = float(cycle_row["avg_cycle_time"]) if cycle_row else 0.0

    # 2. PRs merged in the last 24h
    prs_row = await conn.fetchrow(
        """
        SELECT COUNT(*)::float AS prs_merged_count
        FROM pull_requests
        WHERE team_id = $1
          AND state = 'closed'
          AND merged_at >= (NOW() AT TIME ZONE 'UTC') - INTERVAL '24 hours'
        """,
        team_id,
    )
    prs_merged_count = float(prs_row["prs_merged_count"]) if prs_row else 0.0

    # 3. Tickets opened in the last 24h
    opened_row = await conn.fetchrow(
        """
        SELECT COUNT(*)::float AS tickets_opened_count
        FROM jira_tickets
        WHERE team_id = $1
          AND created_at >= (NOW() AT TIME ZONE 'UTC') - INTERVAL '24 hours'
        """,
        team_id,
    )
    tickets_opened_count = float(opened_row["tickets_opened_count"]) if opened_row else 0.0

    # 4. Tickets resolved in the last 24h
    resolved_row = await conn.fetchrow(
        """
        SELECT COUNT(*)::float AS tickets_resolved_count
        FROM jira_tickets
        WHERE team_id = $1
          AND resolved_at >= (NOW() AT TIME ZONE 'UTC') - INTERVAL '24 hours'
        """,
        team_id,
    )
    tickets_resolved_count = (
        float(resolved_row["tickets_resolved_count"]) if resolved_row else 0.0
    )

    # 5. Stalled PRs (open with no merge for > 72h)
    stalled_row = await conn.fetchrow(
        """
        SELECT COUNT(*)::float AS stalled_prs_count
        FROM pull_requests
        WHERE team_id = $1
          AND state = 'open'
          AND opened_at <= (NOW() AT TIME ZONE 'UTC') - INTERVAL '72 hours'
        """,
        team_id,
    )
    stalled_prs_count = float(stalled_row["stalled_prs_count"]) if stalled_row else 0.0

    return {
        "avg_cycle_time": round(avg_cycle_time, 2),
        "prs_merged_count": prs_merged_count,
        "tickets_opened_count": tickets_opened_count,
        "tickets_resolved_count": tickets_resolved_count,
        "stalled_prs_count": stalled_prs_count,
    }


async def record_team_snapshots(
    conn: asyncpg.Connection,
    team_id: uuid.UUID,
    metrics: dict[str, float],
    recorded_at: datetime | None = None,
) -> list[dict[str, Any]]:
    """Idempotently insert hourly snapshots for each metric."""
    results = []
    for metric_name, val in metrics.items():
        snap = await record_metrics_snapshot(
            conn,
            team_id=team_id,
            metric_name=metric_name,
            value=val,
            recorded_at=recorded_at,
        )
        if snap:
            results.append(snap)
    return results


async def evaluate_metric_anomaly(
    conn: asyncpg.Connection,
    team_id: uuid.UUID,
    metric_name: str,
    redis_client: Any | None = None,
) -> dict[str, Any] | None:
    """Evaluate whether the latest metric reading for a team is anomalous."""
    config = await get_or_create_alert_config(conn, team_id, metric_name)
    snapshots = await fetch_metrics_snapshots(conn, team_id, metric_name, days=30)

    # Guard: minimum data points required before alerting
    if len(snapshots) < config["min_data_points"]:
        logger.debug(
            "anomaly_check_insufficient_data",
            team_id=str(team_id),
            metric=metric_name,
            points=len(snapshots),
            required=config["min_data_points"],
        )
        return None

    # Separate current point from historical baseline
    current_val = snapshots[-1]["value"]
    baseline_vals = [s["value"] for s in snapshots[:-1]]
    if not baseline_vals:
        baseline_vals = [current_val]

    mean, std = calculate_rolling_stats(baseline_vals)
    # Floor std at 0.5 to prevent zero-variance baselines from alarming on normal low-volume fluctuation
    effective_std = max(std, 0.5)
    upper_threshold = mean + (config["std_multiplier"] * effective_std)

    is_anomaly = False
    severity = "warning"

    # Metric specific anomaly rules
    if metric_name == "avg_cycle_time":
        # Cycle time spike: exceeds std band OR > 2x rolling average (when non-trivial)
        if current_val > upper_threshold or (mean > 1.0 and current_val >= 2.0 * mean):
            is_anomaly = True
            if current_val >= mean + (config["std_multiplier"] * 1.5 * effective_std):
                severity = "critical"
    elif metric_name == "stalled_prs_count":
        # PR stall spike
        if current_val > upper_threshold and current_val >= 1.0:
            is_anomaly = True
            if current_val >= 5.0:
                severity = "critical"
    elif (
        metric_name in ("tickets_opened_count", "prs_merged_count", "tickets_resolved_count")
        and current_val > upper_threshold
    ):
        is_anomaly = True

    if not is_anomaly:
        # Metric returned to normal band -> auto-resolve existing active alerts
        resolved_count = await auto_resolve_alerts_for_metric(conn, team_id, metric_name)
        if resolved_count > 0:
            logger.info(
                "alert_auto_resolved",
                team_id=str(team_id),
                metric=metric_name,
                resolved_count=resolved_count,
            )
        return None

    # Anomaly detected: Check suppression window
    recent_alerts = await fetch_recent_alerts_in_window(
        conn, team_id, metric_name, config["suppression_window_minutes"]
    )
    if recent_alerts:
        # Suppress repeated alert
        alert = await create_workflow_alert(
            conn,
            team_id=team_id,
            metric_name=metric_name,
            actual_value=current_val,
            expected_value=round(mean, 2),
            severity=severity,
            status="suppressed",
            details={
                "rolling_mean": round(mean, 2),
                "rolling_std": round(std, 2),
                "upper_threshold": round(upper_threshold, 2),
                "suppressed": True,
            },
        )
        logger.info(
            "alert_suppressed",
            team_id=str(team_id),
            metric=metric_name,
            window_minutes=config["suppression_window_minutes"],
        )
        return alert

    # Active new alert
    alert = await create_workflow_alert(
        conn,
        team_id=team_id,
        metric_name=metric_name,
        actual_value=current_val,
        expected_value=round(mean, 2),
        severity=severity,
        status="active",
        details={
            "rolling_mean": round(mean, 2),
            "rolling_std": round(std, 2),
            "upper_threshold": round(upper_threshold, 2),
        },
    )

    # Check alert grouping: group if 3+ alerts occur within 5 minutes
    grouped_id = await group_recent_alerts(conn, team_id, window_minutes=5, min_alerts=3)
    if grouped_id:
        alert["group_id"] = grouped_id
        alert["severity"] = "critical"
        alert["status"] = "grouped"
        logger.warning(
            "alerts_grouped_critical",
            team_id=str(team_id),
            group_id=grouped_id,
        )

    # Broadcast via Redis Pub/Sub
    if redis_client is not None:
        await publish_workflow_alert(redis_client, alert)

    logger.warning(
        "workflow_anomaly_detected",
        team_id=str(team_id),
        metric=metric_name,
        actual=current_val,
        expected=round(mean, 2),
        severity=alert["severity"],
    )
    return alert


async def evaluate_team_anomalies(
    conn: asyncpg.Connection,
    team_id: uuid.UUID,
    redis_client: Any | None = None,
) -> list[dict[str, Any]]:
    """Evaluate all tracked metrics for a team."""
    triggered: list[dict[str, Any]] = []
    for metric_name in TRACKED_METRICS:
        alert = await evaluate_metric_anomaly(
            conn, team_id, metric_name, redis_client=redis_client
        )
        if alert and alert.get("status") in ("active", "grouped"):
            triggered.append(alert)
    return triggered


async def run_snapshot_and_anomaly_cycle(
    db_pool: Any,
    redis_client: Any | None = None,
) -> dict[str, Any]:
    """Full scheduled cycle: compute snapshots and evaluate anomalies for all teams."""
    from app.db.pool import acquire_connection

    summary = {"teams_processed": 0, "snapshots_recorded": 0, "alerts_triggered": 0}

    async with acquire_connection(db_pool, "anomaly_detector") as conn:
        team_rows = await conn.fetch("SELECT id FROM teams")
        team_ids = [row["id"] for row in team_rows]

    for team_id in team_ids:
        async with acquire_connection(db_pool, "anomaly_detector") as conn, conn.transaction():
            metrics = await compute_team_metrics_for_hour(conn, team_id)
            snaps = await record_team_snapshots(conn, team_id, metrics)
            summary["snapshots_recorded"] += len(snaps)

            alerts = await evaluate_team_anomalies(conn, team_id, redis_client=redis_client)
            summary["alerts_triggered"] += len(alerts)
            summary["teams_processed"] += 1

    logger.info("anomaly_cycle_complete", **summary)
    return summary
