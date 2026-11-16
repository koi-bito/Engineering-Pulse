import json
import uuid
from collections.abc import AsyncIterator
from typing import Any

import pytest
from fastapi import WebSocketDisconnect
from fastapi.testclient import TestClient
from httpx import ASGITransport, AsyncClient

from app.auth.backend import (
    REFRESH_COOKIE_NAME,
    current_active_user,
    generate_access_token,
)
from app.auth.manager import UserManager, get_user_manager
from app.config import Settings
from app.main import create_app
from app.models.user import User
from app.services.pubsub import LIVE_FEED_CHANNEL
from tests.conftest import FakeAsyncRedis, FakeKafkaProducer, FakeQueryPool, FakeS3Archive


class InMemoryUserDB:
    def __init__(self) -> None:
        self.users: dict[uuid.UUID, User] = {}
        self.emails: dict[str, uuid.UUID] = {}

    def add_user(self, user: User) -> None:
        self.users[user.id] = user
        self.emails[user.email.lower()] = user.id


class FakeTestUserManager(UserManager):
    def __init__(self, db: InMemoryUserDB) -> None:
        self._db = db

    async def authenticate(self, credentials: Any) -> User | None:
        user_id = self._db.emails.get(credentials.username.lower())
        if not user_id:
            return None
        user = self._db.users.get(user_id)
        if user and credentials.password == "validpassword123":
            return user
        return None

    async def get(self, id: uuid.UUID) -> User:
        user = self._db.users.get(id)
        if not user:
            raise KeyError("User not found")
        return user

    async def get_by_email(self, email: str) -> User | None:
        user_id = self._db.emails.get(email.lower())
        return self._db.users.get(user_id) if user_id else None

    async def create(self, user_create: Any, safe: bool = False, request: Any = None) -> User:
        user = User(
            id=uuid.uuid4(),
            email=user_create.email,
            hashed_password="hashed_" + user_create.password,
            is_active=True,
            is_superuser=False,
            is_verified=True,
            org_id=user_create.org_id,
            team_id=user_create.team_id,
            role=user_create.role or "member",
        )
        self._db.add_user(user)
        return user


@pytest.fixture
def user_db() -> InMemoryUserDB:
    db = InMemoryUserDB()
    user = User(
        id=uuid.UUID("11111111-1111-1111-1111-111111111111"),
        email="developer@acme.corp",
        hashed_password="hashed_validpassword123",
        is_active=True,
        is_superuser=False,
        is_verified=True,
        role="member",
        org_id=uuid.UUID("22222222-2222-2222-2222-222222222222"),
        team_id=uuid.UUID("33333333-3333-3333-3333-333333333333"),
    )
    admin = User(
        id=uuid.UUID("44444444-4444-4444-4444-444444444444"),
        email="lead@acme.corp",
        hashed_password="hashed_validpassword123",
        is_active=True,
        is_superuser=False,
        is_verified=True,
        role="admin",
        org_id=uuid.UUID("22222222-2222-2222-2222-222222222222"),
        team_id=uuid.UUID("33333333-3333-3333-3333-333333333333"),
    )
    db.add_user(user)
    db.add_user(admin)
    return db


@pytest.fixture
def auth_app(user_db: InMemoryUserDB):
    cfg = Settings(
        ENVIRONMENT="local",
        ES_ENABLED=False,
        GITHUB_WEBHOOK_SECRET="s",
        JIRA_WEBHOOK_SECRET="s",
    )
    app = create_app(settings=cfg, rate_limit_storage_uri="memory://")
    app.state.db_pool = FakeQueryPool()
    app.state.redis = FakeAsyncRedis()
    app.state.s3_archive = FakeS3Archive()
    app.state.kafka_producer = FakeKafkaProducer()

    app.dependency_overrides[get_user_manager] = lambda: FakeTestUserManager(user_db)
    return app


@pytest.fixture
async def auth_client(auth_app) -> AsyncIterator[AsyncClient]:
    transport = ASGITransport(app=auth_app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


async def test_login_returns_jwt_and_httponly_refresh_cookie(auth_client: AsyncClient) -> None:
    res = await auth_client.post(
        "/auth/jwt/login",
        data={"username": "developer@acme.corp", "password": "validpassword123"},
    )
    assert res.status_code == 200
    data = res.json()
    assert "access_token" in data
    assert data["token_type"] == "bearer"
    assert data["user"]["email"] == "developer@acme.corp"

    # Verify httpOnly refresh cookie
    cookie = res.cookies.get(REFRESH_COOKIE_NAME)
    assert cookie is not None
    assert len(cookie) > 20


async def test_refresh_token_issues_new_access_token_and_rotates_cookie(
    auth_client: AsyncClient,
) -> None:
    login_res = await auth_client.post(
        "/auth/jwt/login",
        data={"username": "developer@acme.corp", "password": "validpassword123"},
    )
    refresh_cookie = login_res.cookies.get(REFRESH_COOKIE_NAME)
    assert refresh_cookie is not None

    refresh_res = await auth_client.post(
        "/auth/jwt/refresh",
        cookies={REFRESH_COOKIE_NAME: refresh_cookie},
    )
    assert refresh_res.status_code == 200
    data = refresh_res.json()
    assert "access_token" in data
    new_cookie = refresh_res.cookies.get(REFRESH_COOKIE_NAME)
    assert new_cookie is not None


async def test_refresh_token_missing_or_invalid_returns_401(auth_client: AsyncClient) -> None:
    res = await auth_client.post("/auth/jwt/refresh")
    assert res.status_code == 401
    assert "Missing refresh token" in res.json()["detail"]

    bad_res = await auth_client.post(
        "/auth/jwt/refresh",
        cookies={REFRESH_COOKIE_NAME: "invalid-token"},
    )
    assert bad_res.status_code == 401


async def test_protected_endpoints_without_token_return_401(auth_client: AsyncClient) -> None:
    res1 = await auth_client.get("/api/metrics/summary")
    assert res1.status_code == 401

    res2 = await auth_client.get("/api/sprint/active")
    assert res2.status_code == 401

    res3 = await auth_client.get("/api/search?q=test")
    assert res3.status_code == 401


async def test_protected_endpoints_with_valid_token_return_200(
    auth_app, auth_client: AsyncClient, user_db: InMemoryUserDB
) -> None:
    user = user_db.users[uuid.UUID("11111111-1111-1111-1111-111111111111")]
    token = generate_access_token(user)

    auth_app.dependency_overrides[current_active_user] = lambda: user

    res = await auth_client.get(
        "/api/metrics/summary",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert res.status_code == 200
    data = res.json()
    assert "prs_merged_today" in data


async def test_brute_force_login_rate_limiting_returns_429(auth_client: AsyncClient) -> None:
    # AUTH_LOGIN_RATE_LIMIT is 10/minute
    for _ in range(10):
        await auth_client.post(
            "/auth/jwt/login",
            data={"username": "developer@acme.corp", "password": "wrongpassword"},
        )

    blocked_res = await auth_client.post(
        "/auth/jwt/login",
        data={"username": "developer@acme.corp", "password": "wrongpassword"},
    )
    assert blocked_res.status_code == 429
    assert blocked_res.json()["error"] == "rate_limit_exceeded"
    assert "Retry-After" in blocked_res.headers


async def test_register_creates_org_team_and_admin_user(auth_client: AsyncClient) -> None:
    payload = {
        "email": "cto@newstartup.io",
        "password": "strongPassword123!",
        "org_name": "NewStartup Corp",
        "team_name": "Core Platform",
    }
    res = await auth_client.post("/api/auth/register", json=payload)
    assert res.status_code == 201
    user = res.json()
    assert user["email"] == "cto@newstartup.io"
    assert user["role"] == "admin"
    assert user["org_id"] is not None
    assert user["team_id"] is not None


async def test_role_check_dependency_admin_vs_member(
    auth_app, auth_client: AsyncClient, user_db: InMemoryUserDB
) -> None:
    member = user_db.users[uuid.UUID("11111111-1111-1111-1111-111111111111")]
    admin = user_db.users[uuid.UUID("44444444-4444-4444-4444-444444444444")]

    # Member user is forbidden
    auth_app.dependency_overrides[current_active_user] = lambda: member
    res_member = await auth_client.get(
        "/api/auth/admin-check",
        headers={"Authorization": "Bearer member-token"},
    )
    assert res_member.status_code == 403

    # Admin user is allowed
    auth_app.dependency_overrides[current_active_user] = lambda: admin
    res_admin = await auth_client.get(
        "/api/auth/admin-check",
        headers={"Authorization": "Bearer admin-token"},
    )
    assert res_admin.status_code == 200
    assert res_admin.json() == {"status": "authorized", "role": "admin"}


def test_ws_feed_requires_auth_token_in_first_message(auth_app, user_db: InMemoryUserDB) -> None:
    user = user_db.users[uuid.UUID("11111111-1111-1111-1111-111111111111")]
    valid_token = generate_access_token(user)

    client = TestClient(auth_app)

    # 1. Unauthenticated or invalid first frame should be rejected with 4401
    with client.websocket_connect("/ws/feed") as ws:
        ws.send_json({"type": "bad_type", "token": "foo"})
        # Expect immediate close with 4401
        with pytest.raises(WebSocketDisconnect) as exc_info:
            ws.receive_json()
        assert exc_info.value.code == 4401

    # 2. Valid auth frame connects and streams
    with client.websocket_connect("/ws/feed") as ws:
        ws.send_json({"type": "auth", "token": valid_token})

        auth_app.state.redis.publish_sync(
            LIVE_FEED_CHANNEL,
            json.dumps(
                {
                    "type": "event",
                    "source": "github",
                    "event_type": "pull_request",
                    "summary": "PR #1 merged",
                    "timestamp": "2026-11-16T12:00:00Z",
                }
            ),
        )
        msg = ws.receive_json()
        assert msg["type"] == "event"
        assert msg["data"]["summary"] == "PR #1 merged"
