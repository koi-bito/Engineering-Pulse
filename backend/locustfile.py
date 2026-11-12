"""Locust load-test suite for Engineering Pulse webhook and read API endpoints."""

import hashlib
import hmac
import json
import os
import uuid
from datetime import UTC, datetime

from locust import HttpUser, between, task

GITHUB_SECRET = os.getenv("GITHUB_WEBHOOK_SECRET", "local-github-secret")


def _sign_github(raw_body: bytes) -> str:
    digest = hmac.new(GITHUB_SECRET.encode("utf-8"), raw_body, hashlib.sha256).hexdigest()
    return f"sha256={digest}"


class EngineeringPulseUser(HttpUser):
    wait_time = between(0.5, 2.0)

    @task(4)
    def get_summary_metrics(self) -> None:
        self.client.get("/api/metrics/summary", name="/api/metrics/summary")

    @task(2)
    def get_pr_throughput(self) -> None:
        self.client.get("/api/metrics/prs?range=7d", name="/api/metrics/prs")

    @task(2)
    def get_cycle_time(self) -> None:
        self.client.get("/api/metrics/cycle-time?range=30d", name="/api/metrics/cycle-time")

    @task(1)
    def get_active_sprint(self) -> None:
        self.client.get("/api/sprint/active", name="/api/sprint/active")

    @task(1)
    def get_health(self) -> None:
        self.client.get("/health", name="/health")

    @task(2)
    def post_github_webhook(self) -> None:
        now = datetime.now(UTC).isoformat().replace("+00:00", "Z")
        delivery_id = str(uuid.uuid4())
        payload = {
            "action": "opened",
            "repository": {"full_name": "acme/load-test"},
            "pull_request": {
                "id": uuid.uuid4().int % 1_000_000,
                "title": "Load test pull request",
                "state": "open",
                "user": {"login": "locust"},
                "created_at": now,
                "merged_at": None,
            },
        }
        raw_body = json.dumps(payload).encode("utf-8")
        self.client.post(
            "/webhooks/github",
            data=raw_body,
            headers={
                "Content-Type": "application/json",
                "X-Hub-Signature-256": _sign_github(raw_body),
                "X-GitHub-Delivery": delivery_id,
                "X-GitHub-Event": "pull_request",
            },
            name="/webhooks/github",
        )

