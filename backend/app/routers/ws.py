import asyncio
import contextlib
import json
from typing import Any

import structlog
from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from app.auth.backend import decode_auth_token
from app.services.pubsub import LIVE_FEED_CHANNEL, team_alerts_channel

logger = structlog.get_logger(__name__)

router = APIRouter(tags=["websocket"])


def _normalize_ws_message(raw_data: str) -> str:
    try:
        parsed = json.loads(raw_data)
    except json.JSONDecodeError:
        return json.dumps({"type": "event", "data": {"raw": raw_data}})
    if not isinstance(parsed, dict):
        return json.dumps({"type": "event", "data": {"value": parsed}})

    parsed.setdefault("type", "event")
    if "data" not in parsed:
        data: dict[str, Any] = {}
        if "summary" in parsed:
            data["summary"] = parsed["summary"]
        if "timestamp" in parsed:
            data["timestamp"] = parsed["timestamp"]
        parsed["data"] = data
    return json.dumps(parsed)


async def _forward_pubsub(websocket: WebSocket, pubsub: Any) -> None:
    user_org_id = getattr(websocket.state, "org_id", None)
    user_team_id = getattr(websocket.state, "team_id", None)
    while True:
        message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=0.5)
        if message is None:
            await asyncio.sleep(0)
            continue
        raw_data = message.get("data")
        if isinstance(raw_data, bytes):
            raw_data = raw_data.decode("utf-8")
        if not isinstance(raw_data, str):
            continue

        # Enforce tenant isolation: drop messages belonging to other orgs or teams
        try:
            parsed = json.loads(raw_data)
            if isinstance(parsed, dict):
                evt_org = parsed.get("org_id") or parsed.get("data", {}).get("org_id")
                evt_team = parsed.get("team_id") or parsed.get("data", {}).get("team_id")
                if user_org_id and evt_org and str(evt_org) != str(user_org_id):
                    continue
                if user_team_id and evt_team and str(evt_team) != str(user_team_id):
                    continue
        except (json.JSONDecodeError, TypeError):
            pass

        await websocket.send_text(_normalize_ws_message(raw_data))


async def _receive_client_messages(websocket: WebSocket) -> None:
    while True:
        raw = await websocket.receive_text()
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict) and payload.get("type") == "ping":
            await websocket.send_json({"type": "pong"})


@router.websocket("/ws/feed")
async def websocket_feed(websocket: WebSocket) -> None:
    await websocket.accept()

    # Phase 3A: Require first message to be { "type": "auth", "token": "<access_token>" }
    try:
        raw_first = await asyncio.wait_for(websocket.receive_text(), timeout=5.0)
        auth_payload = json.loads(raw_first)
        if (
            not isinstance(auth_payload, dict)
            or auth_payload.get("type") != "auth"
            or not auth_payload.get("token")
        ):
            logger.warning("ws_auth_rejected", reason="invalid_auth_payload")
            await websocket.close(code=4401, reason="Unauthorized: Expected auth frame")
            return

        token = str(auth_payload["token"])
        claims = decode_auth_token(token)
    except (TimeoutError, Exception) as exc:  # noqa: BLE001
        logger.warning("ws_auth_failed", error=str(exc))
        with contextlib.suppress(Exception):
            await websocket.close(code=4401, reason="Unauthorized")
        return

    # User authenticated
    websocket.state.user_id = claims.get("sub")
    websocket.state.org_id = claims.get("org_id")
    websocket.state.team_id = claims.get("team_id")
    websocket.state.role = claims.get("role")

    connections: set[WebSocket] | None = getattr(websocket.app.state, "ws_connections", None)
    if connections is not None:
        connections.add(websocket)

    redis_client = websocket.app.state.redis
    pubsub = redis_client.pubsub()
    channels = [LIVE_FEED_CHANNEL]
    if websocket.state.team_id:
        channels.append(f"{LIVE_FEED_CHANNEL}:{websocket.state.team_id}")
        channels.append(team_alerts_channel(websocket.state.team_id))
    await pubsub.subscribe(*channels)
    logger.info(
        "ws_client_connected",
        channels=channels,
        user_id=claims.get("sub"),
    )

    receiver = asyncio.create_task(_receive_client_messages(websocket))
    forwarder = asyncio.create_task(_forward_pubsub(websocket, pubsub))
    try:
        done, pending = await asyncio.wait(
            {receiver, forwarder},
            return_when=asyncio.FIRST_COMPLETED,
        )
        for task in pending:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        for task in done:
            exc = task.exception()
            if exc is not None and not isinstance(exc, WebSocketDisconnect):
                raise exc
    except WebSocketDisconnect:
        pass
    finally:
        if connections is not None:
            connections.discard(websocket)
        with contextlib.suppress(Exception):
            await pubsub.unsubscribe(*channels)
        with contextlib.suppress(Exception):
            await pubsub.aclose()
        logger.info("ws_client_disconnected", channels=channels)
