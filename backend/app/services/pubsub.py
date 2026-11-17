import json
from datetime import UTC, datetime
from typing import Any

LIVE_FEED_CHANNEL = "live-feed"


def _to_utc_iso(value: datetime) -> str:
    utc_dt = value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
    return utc_dt.isoformat().replace("+00:00", "Z")


async def publish_live_event(
    redis_client: Any,
    *,
    source: str,
    event_type: str,
    summary: str,
    timestamp: datetime,
    team_id: str | None = None,
    org_id: str | None = None,
    data: dict[str, Any] | None = None,
) -> None:
    iso_timestamp = _to_utc_iso(timestamp)
    payload_data = {
        "summary": summary,
        "timestamp": iso_timestamp,
        "team_id": team_id,
        "org_id": org_id,
        **(data or {}),
    }
    message = {
        "type": "event",
        "source": source,
        "event_type": event_type,
        "summary": summary,
        "timestamp": iso_timestamp,
        "team_id": team_id,
        "org_id": org_id,
        "data": payload_data,
    }
    msg_json = json.dumps(message)
    await redis_client.publish(LIVE_FEED_CHANNEL, msg_json)
    if team_id:
        await redis_client.publish(f"{LIVE_FEED_CHANNEL}:{team_id}", msg_json)


def team_alerts_channel(team_id: str | None = None) -> str:
    return f"alerts:{team_id}" if team_id else "alerts"


async def publish_workflow_alert(
    redis_client: Any,
    alert: dict[str, Any],
) -> None:
    team_id = str(alert.get("team_id")) if alert.get("team_id") else None
    now_iso = datetime.now(UTC).isoformat().replace("+00:00", "Z")
    message = {
        "type": "alert",
        "source": "anomaly_detector",
        "event_type": "workflow_alert",
        "summary": f"[{alert.get('severity', 'warning').upper()}] {alert.get('metric_name')}: actual {alert.get('actual_value')} vs expected {alert.get('expected_value')}",
        "timestamp": alert.get("created_at") or now_iso,
        "team_id": team_id,
        "data": {
            "alert": alert,
            "metric_name": alert.get("metric_name"),
            "actual_value": alert.get("actual_value"),
            "expected_value": alert.get("expected_value"),
            "severity": alert.get("severity"),
            "status": alert.get("status"),
            "group_id": str(alert.get("group_id")) if alert.get("group_id") else None,
            "details": alert.get("details") or {},
        },
    }
    encoded = json.dumps(message, default=str)
    await redis_client.publish(LIVE_FEED_CHANNEL, encoded)
    if team_id:
        await redis_client.publish(f"{LIVE_FEED_CHANNEL}:{team_id}", encoded)
        await redis_client.publish(team_alerts_channel(team_id), encoded)
