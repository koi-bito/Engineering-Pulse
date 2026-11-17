import uuid
from datetime import UTC, datetime, timedelta

import pytest

from app.services.anomaly_detector import calculate_rolling_stats, evaluate_metric_anomaly
from tests.conftest import FakeAsyncRedis

TEAM_ID = uuid.UUID("33333333-3333-3333-3333-333333333333")


def test_calculate_rolling_stats_empty_and_single() -> None:
    mean, std = calculate_rolling_stats([])
    assert mean == 0.0
    assert std == 0.0

    mean_single, std_single = calculate_rolling_stats([42.0])
    assert mean_single == 42.0
    assert std_single == 0.0


def test_calculate_rolling_stats_known_values() -> None:
    values = [10.0, 12.0, 14.0, 16.0, 18.0]
    mean, std = calculate_rolling_stats(values)
    assert mean == 14.0
    # Sample variance: ((16+4+0+4+16)/4) = 10 -> std = sqrt(10) ~= 3.162
    assert round(std, 3) == 3.162


class FakeAnomalyConn:
    def __init__(self) -> None:
        self.snapshots: list[dict] = []
        self.recent_alerts: list[dict] = []
        self.created_alerts: list[dict] = []
        self.config = {
            "std_multiplier": 2.0,
            "min_data_points": 7,
            "suppression_window_minutes": 60,
        }

    async def fetchrow(self, query: str, *args):
        if "FROM alert_configs" in query:
            return {
                "id": uuid.uuid4(),
                "team_id": args[0],
                "metric_name": args[1],
                "std_multiplier": self.config["std_multiplier"],
                "min_data_points": self.config["min_data_points"],
                "suppression_window_minutes": self.config["suppression_window_minutes"],
                "created_at": datetime.now(UTC),
                "updated_at": datetime.now(UTC),
            }
        if "INSERT INTO workflow_alerts" in query:
            alert = {
                "id": args[0],
                "team_id": args[1],
                "metric_name": args[2],
                "actual_value": args[3],
                "expected_value": args[4],
                "severity": args[5],
                "status": args[6],
                "group_id": args[7],
                "details": args[8],
                "created_at": datetime.now(UTC),
            }
            self.created_alerts.append(alert)
            return alert
        return None

    async def fetch(self, query: str, *args):
        if "FROM metrics_snapshots" in query:
            return self.snapshots
        if "FROM workflow_alerts" in query:
            return self.recent_alerts
        return []

    async def execute(self, query: str, *args):
        if "UPDATE workflow_alerts" in query:
            return "UPDATE 1"
        return "EXECUTE 1"


@pytest.mark.asyncio
async def test_insufficient_data_guard() -> None:
    conn = FakeAnomalyConn()
    # Only 3 snapshots provided, but min_data_points is 7
    conn.snapshots = [
        {"id": uuid.uuid4(), "team_id": TEAM_ID, "metric_name": "avg_cycle_time", "value": 5.0, "recorded_at": datetime.now(UTC)},
        {"id": uuid.uuid4(), "team_id": TEAM_ID, "metric_name": "avg_cycle_time", "value": 5.2, "recorded_at": datetime.now(UTC)},
        {"id": uuid.uuid4(), "team_id": TEAM_ID, "metric_name": "avg_cycle_time", "value": 15.0, "recorded_at": datetime.now(UTC)},
    ]
    alert = await evaluate_metric_anomaly(conn, TEAM_ID, "avg_cycle_time")
    assert alert is None
    assert len(conn.created_alerts) == 0


@pytest.mark.asyncio
async def test_cycle_time_spike_triggers_active_alert() -> None:
    conn = FakeAnomalyConn()
    now = datetime.now(UTC)
    # 8 steady snapshots (~4.0 hours), then a spike to 25.0 hours
    for i in range(8):
        conn.snapshots.append({
            "id": uuid.uuid4(),
            "team_id": TEAM_ID,
            "metric_name": "avg_cycle_time",
            "value": 4.0 + (i % 2) * 0.2,
            "recorded_at": now - timedelta(days=8 - i),
        })
    conn.snapshots.append({
        "id": uuid.uuid4(),
        "team_id": TEAM_ID,
        "metric_name": "avg_cycle_time",
        "value": 25.0,
        "recorded_at": now,
    })

    redis = FakeAsyncRedis()
    alert = await evaluate_metric_anomaly(conn, TEAM_ID, "avg_cycle_time", redis_client=redis)
    assert alert is not None
    assert alert["status"] == "active"
    assert alert["actual_value"] == 25.0
    assert alert["metric_name"] == "avg_cycle_time"
    assert len(conn.created_alerts) == 1
    # Check published to Redis
    assert len(redis.published) > 0


@pytest.mark.asyncio
async def test_alert_suppression_window() -> None:
    conn = FakeAnomalyConn()
    now = datetime.now(UTC)
    for i in range(8):
        conn.snapshots.append({
            "id": uuid.uuid4(),
            "team_id": TEAM_ID,
            "metric_name": "avg_cycle_time",
            "value": 4.0,
            "recorded_at": now - timedelta(days=8 - i),
        })
    conn.snapshots.append({
        "id": uuid.uuid4(),
        "team_id": TEAM_ID,
        "metric_name": "avg_cycle_time",
        "value": 25.0,
        "recorded_at": now,
    })

    # Simulate an alert was already triggered 10 minutes ago (< 60m suppression window)
    conn.recent_alerts = [{
        "id": uuid.uuid4(),
        "team_id": TEAM_ID,
        "metric_name": "avg_cycle_time",
        "created_at": now - timedelta(minutes=10),
    }]

    alert = await evaluate_metric_anomaly(conn, TEAM_ID, "avg_cycle_time")
    assert alert is not None
    assert alert["status"] == "suppressed"
