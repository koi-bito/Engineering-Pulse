import asyncio
import contextlib
import json
from typing import Any

import structlog
from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from app.services.pubsub import LIVE_FEED_CHANNEL

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
        await websocket.send_text(_normalize_ws_message(raw_data))


async def _receive_client_messages(websocket: WebSocket) -> None:
    while True:
        raw = await websocket.receive_text()
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict) and payload.get("type") == "auth":
            # Phase 3A will enforce JWT validation on the initial auth message.
            continue


@router.websocket("/ws/feed")
async def websocket_feed(websocket: WebSocket) -> None:
    await websocket.accept()
    connections: set[WebSocket] | None = getattr(websocket.app.state, "ws_connections", None)
    if connections is not None:
        connections.add(websocket)

    redis_client = websocket.app.state.redis
    pubsub = redis_client.pubsub()
    await pubsub.subscribe(LIVE_FEED_CHANNEL)
    logger.info("ws_client_connected", channel=LIVE_FEED_CHANNEL)

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
            await pubsub.unsubscribe(LIVE_FEED_CHANNEL)
        with contextlib.suppress(Exception):
            await pubsub.aclose()
        logger.info("ws_client_disconnected", channel=LIVE_FEED_CHANNEL)

