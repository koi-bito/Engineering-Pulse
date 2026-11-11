from datetime import datetime

from httpx import ASGITransport, AsyncClient

from app.config import Settings
from app.main import create_app


async def test_health_returns_utc_timestamp() -> None:
    app = create_app()
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get("/health", headers={"Origin": "http://localhost:5173"})

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "ok"
    assert body["timestamp"].endswith("Z")
    datetime.fromisoformat(body["timestamp"])
    assert response.headers["access-control-allow-origin"] == "http://localhost:5173"
    assert response.headers["access-control-allow-credentials"] == "true"


def test_settings_environment_parity() -> None:
    local = Settings(ENVIRONMENT="local", AWS_ENDPOINT_URL="http://localhost:4566")
    prod = Settings(ENVIRONMENT="production", AWS_ENDPOINT_URL="http://localhost:4566")

    assert local.s3_endpoint_url == "http://localhost:4566"
    assert prod.s3_endpoint_url is None
    assert "http://127.0.0.1:5173" in local.cors_origins
    assert "http://127.0.0.1:5173" not in prod.cors_origins
