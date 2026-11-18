import json
import uuid
from collections.abc import AsyncIterator

import pytest
from httpx import ASGITransport, AsyncClient

from app.auth.backend import generate_access_token
from app.auth.manager import UserManager, get_user_manager
from app.config import Settings
from app.main import create_app
from app.models.user import User
from app.services.cache import metrics_summary_key
from tests.conftest import (
    FakeAsyncRedis,
    FakeKafkaProducer,
    FakeQueryPool,
    FakeS3Archive,
    sign_github_payload,
)

ORG_A = uuid.UUID("11111111-1111-4000-8000-000000000001")
TEAM_A1 = uuid.UUID("11111111-1111-4000-8000-000000000011")
TEAM_A2 = uuid.UUID("11111111-1111-4000-8000-000000000012")

ORG_B = uuid.UUID("22222222-2222-4000-8000-000000000002")
TEAM_B1 = uuid.UUID("22222222-2222-4000-8000-000000000021")


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

    async def get(self, id: uuid.UUID) -> User:
        user = self._db.users.get(id)
        if not user:
            raise KeyError("User not found")
        return user


@pytest.fixture
def multi_user_db() -> InMemoryUserDB:
    db = InMemoryUserDB()
    # Org A Admin
    db.add_user(
        User(
            id=uuid.UUID("aaaaaaaa-0000-0000-0000-000000000001"),
            email="admin@orga.com",
            hashed_password="hash",
            is_active=True,
            is_superuser=False,
            is_verified=True,
            role="admin",
            org_id=ORG_A,
            team_id=TEAM_A1,
        )
    )
    # Org A Member
    db.add_user(
        User(
            id=uuid.UUID("aaaaaaaa-0000-0000-0000-000000000002"),
            email="dev@orga.com",
            hashed_password="hash",
            is_active=True,
            is_superuser=False,
            is_verified=True,
            role="member",
            org_id=ORG_A,
            team_id=TEAM_A1,
        )
    )
    # Org B Admin
    db.add_user(
        User(
            id=uuid.UUID("bbbbbbbb-0000-0000-0000-000000000001"),
            email="admin@orgb.com",
            hashed_password="hash",
            is_active=True,
            is_superuser=False,
            is_verified=True,
            role="admin",
            org_id=ORG_B,
            team_id=TEAM_B1,
        )
    )
    return db


@pytest.fixture
def multi_app(multi_user_db: InMemoryUserDB):
    cfg = Settings(
        ENVIRONMENT="local",
        ES_ENABLED=False,
        GITHUB_WEBHOOK_SECRET="local-github-secret",
        JIRA_WEBHOOK_SECRET="local-jira-secret",
        WEBHOOK_RATE_LIMIT="1000/minute",
    )
    app = create_app(settings=cfg, rate_limit_storage_uri="memory://")
    app.state.db_pool = FakeQueryPool()
    app.state.redis = FakeAsyncRedis()
    app.state.s3_archive = FakeS3Archive()
    app.state.kafka_producer = FakeKafkaProducer()

    app.dependency_overrides[get_user_manager] = lambda: FakeTestUserManager(multi_user_db)
    return app


@pytest.fixture
async def multi_client(multi_app) -> AsyncIterator[AsyncClient]:
    transport = ASGITransport(app=multi_app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


@pytest.mark.asyncio
async def test_team_listing_and_admin_creation(
    multi_client: AsyncClient, multi_user_db: InMemoryUserDB, multi_app
) -> None:
    admin_a = multi_user_db.users[uuid.UUID("aaaaaaaa-0000-0000-0000-000000000001")]
    member_a = multi_user_db.users[uuid.UUID("aaaaaaaa-0000-0000-0000-000000000002")]
    admin_token = generate_access_token(admin_a)
    member_token = generate_access_token(member_a)

    # 1. List teams for Org A
    res = await multi_client.get(
        "/api/teams",
        headers={"Authorization": f"Bearer {admin_token}"},
    )
    assert res.status_code == 200
    assert isinstance(res.json(), list)

    # 2. Admin creates a new team
    res_create = await multi_client.post(
        "/api/teams",
        headers={"Authorization": f"Bearer {admin_token}"},
        json={"name": "New Platform Team"},
    )
    assert res_create.status_code == 201
    created = res_create.json()
    assert created["name"] == "New Platform Team"
    assert created["org_id"] == str(ORG_A)

    # 3. Non-admin member cannot create a team (403 Forbidden)
    res_forbidden = await multi_client.post(
        "/api/teams",
        headers={"Authorization": f"Bearer {member_token}"},
        json={"name": "Unauthorized Team"},
    )
    assert res_forbidden.status_code == 403


@pytest.mark.asyncio
async def test_webhook_registration_permissions(
    multi_client: AsyncClient, multi_user_db: InMemoryUserDB
) -> None:
    admin_a = multi_user_db.users[uuid.UUID("aaaaaaaa-0000-0000-0000-000000000001")]
    member_a = multi_user_db.users[uuid.UUID("aaaaaaaa-0000-0000-0000-000000000002")]
    admin_token = generate_access_token(admin_a)
    member_token = generate_access_token(member_a)

    # 1. Admin A registers a webhook secret for Org A / Team A1
    payload = {
        "team_id": str(TEAM_A1),
        "source": "github",
        "secret": "super-secret-key-12345",
    }
    res = await multi_client.post(
        f"/api/orgs/{ORG_A}/webhooks/register",
        headers={"Authorization": f"Bearer {admin_token}"},
        json=payload,
    )
    assert res.status_code == 201
    data = res.json()
    assert data["org_id"] == str(ORG_A)
    assert data["team_id"] == str(TEAM_A1)
    assert data["source"] == "github"

    # 2. Member A attempts to register (403 Forbidden)
    res_member = await multi_client.post(
        f"/api/orgs/{ORG_A}/webhooks/register",
        headers={"Authorization": f"Bearer {member_token}"},
        json=payload,
    )
    assert res_member.status_code == 403

    # 3. Admin A attempts to register for Org B (403 Forbidden)
    res_cross = await multi_client.post(
        f"/api/orgs/{ORG_B}/webhooks/register",
        headers={"Authorization": f"Bearer {admin_token}"},
        json=payload,
    )
    assert res_cross.status_code == 403


@pytest.mark.asyncio
async def test_webhook_ingestion_tenant_attribution(
    multi_client: AsyncClient, multi_app
) -> None:
    body = {
        "action": "opened",
        "repository": {"full_name": "acme/backend"},
        "pull_request": {
            "id": 501,
            "title": "Tenant test PR",
            "state": "open",
            "user": {"login": "alice"},
            "created_at": "2026-03-01T10:00:00Z",
        },
    }
    raw = json.dumps(body).encode("utf-8")
    sig = sign_github_payload(raw, "local-github-secret")

    res = await multi_client.post(
        "/webhooks/github",
        content=raw,
        headers={
            "Content-Type": "application/json",
            "X-Hub-Signature-256": sig,
            "X-GitHub-Delivery": "delivery-multi-1",
            "X-GitHub-Event": "pull_request",
            "X-Org-ID": str(ORG_A),
            "X-Team-ID": str(TEAM_A1),
        },
    )
    assert res.status_code == 202

    # Check published Kafka envelope has org_id and team_id preserved
    kafka = multi_app.state.kafka_producer
    assert len(kafka.published) > 0
    envelope = kafka.published[-1]
    assert envelope.org_id == str(ORG_A)
    assert envelope.team_id == str(TEAM_A1)


@pytest.mark.asyncio
async def test_tenant_isolation_zero_leakage_queries(
    multi_client: AsyncClient, multi_user_db: InMemoryUserDB, multi_app
) -> None:
    admin_a = multi_user_db.users[uuid.UUID("aaaaaaaa-0000-0000-0000-000000000001")]
    admin_b = multi_user_db.users[uuid.UUID("bbbbbbbb-0000-0000-0000-000000000001")]
    token_a = generate_access_token(admin_a)
    token_b = generate_access_token(admin_b)

    fake_conn = multi_app.state.db_pool.conn

    # 1. User A requests summary
    res_a = await multi_client.get(
        "/api/metrics/summary",
        headers={"Authorization": f"Bearer {token_a}"},
    )
    assert res_a.status_code == 200

    # Verify SQL query executed by User A filtered by team_id of Team A1
    _, last_args = fake_conn.queries[-1]
    assert TEAM_A1 in last_args or str(TEAM_A1) in [str(a) for a in last_args]

    # Verify Redis cache key for Org A / Team A1
    redis = multi_app.state.redis
    expected_key_a = metrics_summary_key(ORG_A, TEAM_A1)
    assert expected_key_a in redis.store

    # 2. User B requests summary
    res_b = await multi_client.get(
        "/api/metrics/summary",
        headers={"Authorization": f"Bearer {token_b}"},
    )
    assert res_b.status_code == 200

    # Verify SQL query executed by User B filtered by team_id of Team B1
    _, last_args_b = fake_conn.queries[-1]
    assert TEAM_B1 in last_args_b or str(TEAM_B1) in [str(a) for a in last_args_b]
    # And specifically NOT Team A1
    assert TEAM_A1 not in last_args_b

    # Verify Redis cache key for Org B / Team B1 exists separately without collision
    expected_key_b = metrics_summary_key(ORG_B, TEAM_B1)
    assert expected_key_b in redis.store
    assert expected_key_a != expected_key_b


@pytest.mark.asyncio
async def test_context_switching_via_x_team_id_header(
    multi_client: AsyncClient, multi_user_db: InMemoryUserDB, multi_app
) -> None:
    admin_a = multi_user_db.users[uuid.UUID("aaaaaaaa-0000-0000-0000-000000000001")]
    token_a = generate_access_token(admin_a)
    fake_conn = multi_app.state.db_pool.conn

    # Normal request -> uses default team A1
    await multi_client.get(
        "/api/metrics/prs?range=7d",
        headers={"Authorization": f"Bearer {token_a}"},
    )
    _, args1 = fake_conn.queries[-1]
    assert str(TEAM_A1) in [str(a) for a in args1]

    # Request with X-Team-ID header -> context switches to Team A2
    await multi_client.get(
        "/api/metrics/prs?range=7d",
        headers={
            "Authorization": f"Bearer {token_a}",
            "X-Team-ID": str(TEAM_A2),
        },
    )
    _, args2 = fake_conn.queries[-1]
    assert str(TEAM_A2) in [str(a) for a in args2]
    assert str(TEAM_A1) not in [str(a) for a in args2]


@pytest.mark.asyncio
async def test_all_teams_query_scoped_by_org_id(
    multi_client: AsyncClient, multi_user_db: InMemoryUserDB, multi_app
) -> None:
    admin_no_team = User(
        id=uuid.UUID("aaaaaaaa-0000-0000-0000-000000000099"),
        email="allteams@orga.com",
        hashed_password="hash",
        is_active=True,
        is_superuser=False,
        is_verified=True,
        role="admin",
        org_id=ORG_A,
        team_id=None,
    )
    multi_user_db.add_user(admin_no_team)
    token_a = generate_access_token(admin_no_team)
    fake_conn = multi_app.state.db_pool.conn

    await multi_client.get(
        "/api/metrics/summary",
        headers={"Authorization": f"Bearer {token_a}"},
    )
    _, args = fake_conn.queries[-1]
    assert ORG_A in args or str(ORG_A) in [str(a) for a in args]
    assert args[1] is None
