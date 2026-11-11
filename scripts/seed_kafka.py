"""Direct-to-Kafka seed script (Pass 1) for consumer development and debugging.

Bypasses the webhook receiver, signature checks, S3 raw archive, and Redis
idempotency. For full end-to-end integration tests and 90-day realistic data,
use scripts/seed.py (Pass 2, built in Phase 1.2).
"""

import argparse
import asyncio
import json
import os
import sys
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

from aiokafka import AIOKafkaProducer

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "backend"))

from app.schemas.events import WebhookEnvelope

DEFAULT_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

ALLOWED_TYPES_BY_TOPIC = {
    "github-events": {"pr", "commit"},
    "jira-events": {"ticket"},
}


def build_sample_envelope(event_type: str, index: int, now: datetime) -> WebhookEnvelope:
    event_id = str(uuid.uuid4())
    if event_type == "pr":
        opened_at = now - timedelta(hours=6 + index)
        merged_at = now - timedelta(minutes=index * 5) if index % 2 == 0 else None
        payload = {
            "action": "closed" if merged_at else "opened",
            "repository": {"full_name": f"acme/service-{index % 3 + 1}"},
            "pull_request": {
                "id": 10_000 + index,
                "title": f"Seeded PR #{index + 1}: improve pipeline throughput",
                "state": "closed" if merged_at else "open",
                "user": {"login": f"dev-{index % 4 + 1}"},
                "created_at": opened_at.isoformat().replace("+00:00", "Z"),
                "merged_at": (
                    merged_at.isoformat().replace("+00:00", "Z") if merged_at else None
                ),
            },
        }
        return WebhookEnvelope(
            event_id=event_id,
            source="github",
            type="pull_request",
            received_at=now,
            payload=payload,
        )

    if event_type == "commit":
        sha = uuid.uuid4().hex
        payload = {
            "ref": "refs/heads/main",
            "repository": {"full_name": f"acme/service-{index % 3 + 1}"},
            "commits": [
                {
                    "id": sha,
                    "message": f"feat: seeded commit #{index + 1}",
                    "timestamp": now.isoformat().replace("+00:00", "Z"),
                    "author": {"username": f"dev-{index % 4 + 1}"},
                }
            ],
        }
        return WebhookEnvelope(
            event_id=event_id,
            source="github",
            type="push",
            received_at=now,
            payload=payload,
        )

    created_at = now - timedelta(hours=12 + index)
    resolved_at = now - timedelta(hours=1) if index % 2 == 0 else None
    payload = {
        "webhookEvent": "jira:issue_updated" if resolved_at else "jira:issue_created",
        "webhookEventId": event_id,
        "issue": {
            "key": f"ENG-{100 + index}",
            "fields": {
                "project": {"key": "ENG"},
                "summary": f"Seeded Jira ticket #{index + 1}",
                "status": {"name": "Done" if resolved_at else "In Progress"},
                "assignee": {"displayName": f"dev-{index % 4 + 1}"},
                "created": created_at.isoformat().replace("+00:00", "Z"),
                "resolutiondate": (
                    resolved_at.isoformat().replace("+00:00", "Z") if resolved_at else None
                ),
            },
        },
    }
    return WebhookEnvelope(
        event_id=event_id,
        source="jira",
        type="jira:issue_updated" if resolved_at else "jira:issue_created",
        received_at=now,
        payload=payload,
    )


async def seed_topic(bootstrap_servers: str, topic: str, event_type: str, count: int) -> None:
    producer = AIOKafkaProducer(bootstrap_servers=bootstrap_servers)
    await producer.start()
    try:
        now = datetime.now(UTC)
        for idx in range(count):
            envelope = build_sample_envelope(event_type, idx, now)
            encoded = json.dumps(envelope.model_dump(mode="json")).encode("utf-8")
            await producer.send_and_wait(
                topic,
                value=encoded,
                key=envelope.event_id.encode("utf-8"),
            )
        print(f"Published {count} {event_type} event(s) to {topic}")
    finally:
        await producer.stop()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Seed sample events directly into Kafka (consumer dev/debug only).",
        epilog=(
            "When to use:\n"
            "  scripts/seed_kafka.py -> consumer development/debugging "
            "(bypasses webhook, S3, idempotency)\n"
            "  scripts/seed.py       -> full integration testing, 90-day demo data "
            "(via webhook endpoint)"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--count", type=int, default=5, help="Number of messages to publish")
    parser.add_argument(
        "--topic",
        choices=sorted(ALLOWED_TYPES_BY_TOPIC),
        default="github-events",
        help="Target Kafka topic",
    )
    parser.add_argument(
        "--type",
        dest="event_type",
        choices=["pr", "commit", "ticket"],
        default="pr",
        help="Sample event payload type",
    )
    parser.add_argument(
        "--bootstrap-servers",
        default=DEFAULT_BOOTSTRAP,
        help=f"Kafka bootstrap servers (default: {DEFAULT_BOOTSTRAP})",
    )
    args = parser.parse_args()
    if args.count < 1:
        parser.error("--count must be >= 1")
    if args.event_type not in ALLOWED_TYPES_BY_TOPIC[args.topic]:
        allowed = ", ".join(sorted(ALLOWED_TYPES_BY_TOPIC[args.topic]))
        parser.error(f"--type={args.event_type} is not valid for --topic={args.topic} ({allowed})")
    return args


def main() -> None:
    args = parse_args()
    asyncio.run(seed_topic(args.bootstrap_servers, args.topic, args.event_type, args.count))


if __name__ == "__main__":
    main()
