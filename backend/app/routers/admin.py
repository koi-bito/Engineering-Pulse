import asyncio
from typing import Any

import structlog
from fastapi import APIRouter, HTTPException, Request, status
from fastapi.websockets import WebSocket

logger = structlog.get_logger(__name__)

router = APIRouter(prefix="/admin", tags=["admin"])


@router.post("/drain-ws", status_code=status.HTTP_200_OK)
async def drain_websockets(request: Request) -> dict[str, Any]:
    """Gracefully close all active client WebSocket connections before server shutdown.

    Restricted to internal local callers or deploy scripts.
    Sends close frame 4000 ("server restarting") so clients know to reconnect via backoff.
    Waits 2 seconds for clean socket teardown.
    """
    client_host = request.client.host if request.client else None
    is_local = client_host in ("127.0.0.1", "::1", "localhost", "testclient")
    if not is_local:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Forbidden: /admin/drain-ws is restricted to local internal callers",
        )

    raw_conns: set[WebSocket] | None = getattr(request.app.state, "ws_connections", None)
    if not raw_conns:
        return {"status": "drained", "closed_connections": 0}

    connections = list(raw_conns)
    closed = 0
    for ws in connections:
        try:
            await ws.close(code=4000, reason="server restarting")
            closed += 1
        except Exception as exc:  # noqa: BLE001
            logger.debug("ws_drain_close_failed", error=str(exc))

    if closed > 0:
        await asyncio.sleep(2.0)

    logger.info("ws_connections_drained", closed_connections=closed)
    return {"status": "drained", "closed_connections": closed}

