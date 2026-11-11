import json

from httpx import ASGITransport, AsyncClient

from app.config import Settings
from app.main import create_app
from app.schemas.events import WebhookEnvelope
from tests.conftest import (
    FakeAsyncRedis,
    FakeKafkaProducer,
    FakeS3Archive,
    sign_github_payload,
)


async def test_github_webhook_valid_publishes_to_s3_and_kafka(
    webhook_client: AsyncClient,
    test_settings: Settings,
    fake_s3: FakeS3Archive,
    fake_kafka: FakeKafkaProducer,
) -> None:
    body = json.dumps({"action": "opened", "number": 7}).encode("utf-8")
    sig = sign_github_payload(body, test_settings.GITHUB_WEBHOOK_SECRET)

    response = await webhook_client.post(
        "/webhooks/github",
        content=body,
        headers={
            "Content-Type": "application/json",
            "X-Hub-Signature-256": sig,
            "X-GitHub-Delivery": "delivery-1",
            "X-GitHub-Event": "pull_request",
        },
    )

    assert response.status_code == 202
    assert response.json()["deduplicated"] is False
    assert len(fake_s3.calls) == 1
    assert fake_s3.calls[0][0] == "github"
    assert fake_s3.calls[0][1] == "delivery-1"
    assert len(fake_kafka.published) == 1
    envelope = fake_kafka.published[0]
    assert isinstance(envelope, WebhookEnvelope)
    assert envelope.event_id == "delivery-1"
    assert envelope.source == "github"
    assert envelope.type == "pull_request"
    assert envelope.payload == {"action": "opened", "number": 7}


async def test_github_webhook_invalid_signature_returns_401(
    webhook_client: AsyncClient,
    fake_s3: FakeS3Archive,
    fake_kafka: FakeKafkaProducer,
) -> None:
    body = json.dumps({"action": "opened"}).encode("utf-8")

    response = await webhook_client.post(
        "/webhooks/github",
        content=body,
        headers={
            "Content-Type": "application/json",
            "X-Hub-Signature-256": "sha256=deadbeef",
            "X-GitHub-Delivery": "delivery-bad",
            "X-GitHub-Event": "pull_request",
        },
    )

    assert response.status_code == 401
    assert response.json()["error"] == "unauthorized"
    assert len(fake_s3.calls) == 0
    assert len(fake_kafka.published) == 0


async def test_github_webhook_duplicate_event_is_deduplicated(
    webhook_client: AsyncClient,
    test_settings: Settings,
    fake_s3: FakeS3Archive,
    fake_kafka: FakeKafkaProducer,
) -> None:
    body = json.dumps({"action": "opened"}).encode("utf-8")
    sig = sign_github_payload(body, test_settings.GITHUB_WEBHOOK_SECRET)
    headers = {
        "Content-Type": "application/json",
        "X-Hub-Signature-256": sig,
        "X-GitHub-Delivery": "delivery-dup",
        "X-GitHub-Event": "pull_request",
    }

    first = await webhook_client.post("/webhooks/github", content=body, headers=headers)
    second = await webhook_client.post("/webhooks/github", content=body, headers=headers)

    assert first.status_code == 202
    assert first.json()["deduplicated"] is False
    assert second.status_code == 202
    assert second.json()["deduplicated"] is True
    assert len(fake_s3.calls) == 1
    assert len(fake_kafka.published) == 1


async def test_jira_webhook_valid_and_invalid_token(
    webhook_client: AsyncClient,
    test_settings: Settings,
    fake_kafka: FakeKafkaProducer,
) -> None:
    body = json.dumps(
        {"webhookEvent": "jira:issue_created", "webhookEventId": "jira-1"}
    ).encode("utf-8")

    bad = await webhook_client.post(
        "/webhooks/jira",
        content=body,
        headers={"X-Jira-Webhook-Secret": "wrong"},
    )
    assert bad.status_code == 401
    assert len(fake_kafka.published) == 0

    good = await webhook_client.post(
        "/webhooks/jira",
        content=body,
        headers={"X-Jira-Webhook-Secret": test_settings.JIRA_WEBHOOK_SECRET},
    )
    assert good.status_code == 202
    assert len(fake_kafka.published) == 1
    assert fake_kafka.published[0].source == "jira"
    assert fake_kafka.published[0].event_id == "jira-1"


async def test_webhook_rate_limit_enforced_per_secret_with_retry_after() -> None:
    settings = Settings(
        ENVIRONMENT="local",
        GITHUB_WEBHOOK_SECRET="rate-secret",
        JIRA_WEBHOOK_SECRET="jira-secret",
        WEBHOOK_RATE_LIMIT="2/minute",
    )
    app = create_app(settings=settings, rate_limit_storage_uri="memory://")
    app.state.redis = FakeAsyncRedis()
    app.state.s3_archive = FakeS3Archive()
    app.state.kafka_producer = FakeKafkaProducer()

    body = json.dumps({"action": "synchronize"}).encode("utf-8")
    sig = sign_github_payload(body, settings.GITHUB_WEBHOOK_SECRET)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        # Invalid signatures must return 401 without consuming the valid secret's quota.
        bad = await client.post(
            "/webhooks/github",
            content=body,
            headers={"X-Hub-Signature-256": "sha256=wrong", "X-GitHub-Delivery": "bad-1"},
        )
        assert bad.status_code == 401

        for idx in range(2):
            ok = await client.post(
                "/webhooks/github",
                content=body,
                headers={
                    "X-Hub-Signature-256": sig,
                    "X-GitHub-Delivery": f"ok-{idx}",
                    "X-GitHub-Event": "pull_request",
                },
            )
            assert ok.status_code == 202

        limited = await client.post(
            "/webhooks/github",
            content=body,
            headers={
                "X-Hub-Signature-256": sig,
                "X-GitHub-Delivery": "ok-3",
                "X-GitHub-Event": "pull_request",
            },
        )

    assert limited.status_code == 429
    assert "Retry-After" in limited.headers
    payload = limited.json()
    assert payload["error"] == "rate_limit_exceeded"
    assert isinstance(payload["retry_after"], int)


async def test_s3_failure_releases_idempotency_key_for_retry(
    webhook_client: AsyncClient,
    test_settings: Settings,
    fake_s3: FakeS3Archive,
    fake_kafka: FakeKafkaProducer,
) -> None:
    body = json.dumps({"action": "opened"}).encode("utf-8")
    sig = sign_github_payload(body, test_settings.GITHUB_WEBHOOK_SECRET)
    headers = {
        "Content-Type": "application/json",
        "X-Hub-Signature-256": sig,
        "X-GitHub-Delivery": "delivery-retry",
        "X-GitHub-Event": "pull_request",
    }

    fake_s3.fail_next = True
    try:
        failed = await webhook_client.post("/webhooks/github", content=body, headers=headers)
        assert failed.status_code == 500
    except RuntimeError:
        pass

    retried = await webhook_client.post("/webhooks/github", content=body, headers=headers)
    assert retried.status_code == 202
    assert retried.json()["deduplicated"] is False
    assert len(fake_kafka.published) == 1
