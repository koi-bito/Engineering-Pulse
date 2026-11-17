import uuid
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta

import pytest
from httpx import ASGITransport, AsyncClient

from app.auth.backend import generate_access_token
from app.auth.manager import UserManager, get_user_manager
from app.config import Settings
from app.main import create_app
from app.models.user import User
from tests.conftest import FakeAsyncRedis, FakeKafkaProducer, FakeQueryPool, FakeS3Archive

TEAM_A = uuid.UUID("11111111-1111-4000-8000-000000000011")
ORG_A = uuid.UUID("11111111-1111-4000-8000-000000000001")


class InMemoryUserDB:
    def __init__(self) -> None:
        self.users: dict[uuid.UUID, User] = {}

    def add_user(self, user: User) -> None:
        self.users[user.id] = user


class FakeTestUserManager(UserManager):
    def __init__(self, db: InMemoryUserDB) -> None:
        self._db = db

    async def get(self, id: uuid.UUID) -> User:
        user = self._db.users.get(id)
        if not user:
            raise KeyError("User not found")
        return user


@pytest.fixture
def alert_user_db() -> InMemoryUserDB:
    db = InMemoryUserDB()
    # Admin User
    db.add_user(
        User(
            id=uuid.UUID("aaaaaaaa-0000-0000-0000-000000000001"),
            email="admin@pulse.io",
            hashed_password="pw",
            is_active=True,
            is_superuser=False,
            is_verified=True,
            role="admin",
            org_id=ORG_A,
            team_id=TEAM_A,
        )
    )
    # Member User
    db.add_user(
        User(
            id=uuid.UUID("aaaaaaaa-0000-0000-0000-000000000002"),
            email="dev@pulse.io",
            hashed_password="pw",
            is_active=True,
            is_superuser=False,
            is_verified=True,
            role="member",
            org_id=ORG_A,
            team_id=TEAM_A,
        )
    )
    return db


@pytest.fixture
def alert_app(alert_user_db: InMemoryUserDB):
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

    app.dependency_overrides[get_user_manager] = lambda: FakeTestUserManager(alert_user_db)
    return app


@pytest.fixture
async def alert_client(alert_app) -> AsyncIterator[AsyncClient]:
    transport = ASGITransport(app=alert_app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


@pytest.mark.asyncio
async def test_snapshot_health_no_data(alert_client: AsyncClient, alert_app) -> None:
    # conn returns None for latest snapshot
    alert_app.state.db_pool.conn.fetchrow_handler = lambda q, *args: None

    res = await alert_client.get("/api/health/snapshots")
    assert res.status_code == 200
    data = res.json()
    assert data["status"] == "no_data"
    assert data["last_snapshot"] is None


@pytest.mark.asyncio
async def test_snapshot_health_ok_and_stale(alert_client: AsyncClient, alert_app) -> None:
    now = datetime.now(UTC)

    # 1. Recent snapshot (10 minutes old) -> OK (200)
    alert_app.state.db_pool.conn.fetchrow_handler = lambda q, *args: {
        "latest": now - timedelta(minutes=10)
    }
    res_ok = await alert_client.get("/api/health/snapshots")
    assert res_ok.status_code == 200
    assert res_ok.json()["status"] == "ok"

    # 2. Stale snapshot (3 hours old) -> 503 Service Unavailable
    alert_app.state.db_pool.conn.fetchrow_handler = lambda q, *args: {
        "latest": now - timedelta(hours=3)
    }
    res_stale = await alert_client.get("/api/health/snapshots")
    assert res_stale.status_code == 503
    assert res_stale.json()["status"] == "stale"


@pytest.mark.asyncio
async def test_alerts_active_requires_auth(alert_client: AsyncClient) -> None:
    res = await alert_client.get("/api/alerts/active")
    assert res.status_code == 401


@pytest.mark.asyncio
async def test_alerts_active_for_authenticated_team(
    alert_client: AsyncClient, alert_user_db: InMemoryUserDB, alert_app
) -> None:
    member = alert_user_db.users[uuid.UUID("aaaaaaaa-0000-0000-0000-000000000002")]
    token = generate_access_token(member)

    alert_app.state.db_pool.conn.fetch_handler = lambda q, *args: [
        {
            "id": uuid.uuid4(),
            "team_id": TEAM_A,
            "metric_name": "avg_cycle_time",
            "actual_value": 24.5,
            "expected_value": 6.2,
            "severity": "critical",
            "status": "active",
            "group_id": None,
            "details": "{}",
            "created_at": datetime.now(UTC),
            "resolved_at": None,
        }
    ]

    res = await alert_client.get(
        "/api/alerts/active",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert res.status_code == 200
    data = res.json()
    assert len(data) == 1
    assert data[0]["metric_name"] == "avg_cycle_time"
    assert data[0]["severity"] == "critical"
    assert data[0]["actual_value"] == 24.5


@pytest.mark.asyncio
async def test_alert_config_permissions(
    alert_client: AsyncClient, alert_user_db: InMemoryUserDB, alert_app
) -> None:
    admin = alert_user_db.users[uuid.UUID("aaaaaaaa-0000-0000-0000-000000000001")]
    member = alert_user_db.users[uuid.UUID("aaaaaaaa-0000-0000-0000-000000000002")]
    admin_token = generate_access_token(admin)
    member_token = generate_access_token(member)

    # 1. Member cannot update alert config (403 Forbidden)
    res_forbidden = await alert_client.post(
        "/api/alerts/config",
        headers={"Authorization": f"Bearer {member_token}"},
        json={"metric_name": "avg_cycle_time", "std_multiplier": 2.5},
    )
    assert res_forbidden.status_code == 403

    # 2. Admin can update alert config (200 OK)
    alert_app.state.db_pool.conn.fetchrow_handler = lambda q, *args: {
        "id": uuid.uuid4(),
        "team_id": TEAM_A,
        "metric_name": "avg_cycle_time",
        "std_multiplier": 2.5,
        "min_data_points": 10,
        "suppression_window_minutes": 120,
    }

    res_ok = await alert_client.post(
        "/api/alerts/config",
        headers={"Authorization": f"Bearer {admin_token}"},
        json={
            "metric_name": "avg_cycle_time",
            "std_multiplier": 2.5,
            "min_data_points": 10,
            "suppression_window_minutes": 120,
        },
    )
    assert res_ok.status_code == 200
    updated = res_ok.json()
    assert updated["std_multiplier"] == 2.5
    assert updated["min_data_points"] == 10
    assert updated["suppression_window_minutes"] == 120
