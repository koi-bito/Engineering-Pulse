"""Full integration & 90-day realistic event generator (Pass 2).

Generates pull requests (opened, merged, stalled), commits, Jira tickets,
and status transitions over a multi-day window with weekend dips, sprint-end
spikes, and occasional burst deploys, submitting them through the webhook API.

For isolated consumer development/debugging without running the API/S3/Redis,
use scripts/seed_kafka.py (Pass 1) instead.
"""

import argparse
import asyncio
import hashlib
import hmac
import json
import os
import random
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx
from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(REPO_ROOT / ".env")

VOLUME_PER_DAY = {"low": 4, "medium": 10, "high": 24}
AUTHORS = ["alice", "bob", "charlie", "diana", "evan", "fiona"]
PR_TITLES = [
    "Fix auth token refresh race condition",
    "Add Kafka consumer offset commit after write",
    "Optimize Postgres cycle-time aggregation index",
    "Handle Jira timezone offset normalization",
    "Add Elasticsearch camelCase search mapping",
    "Improve Redis Pub/Sub live feed fan-out",
]
COMMIT_MESSAGES = [
    "fix: handle edge case in webhook signature verification",
    "feat: normalize Jira timestamps to UTC",
    "perf: add composite index on opened_at",
    "chore: tune Kafka consumer backoff",
    "refactor: extract shared Postgres upsert helper",
]
JIRA_SUMMARIES = [
    "Investigate elevated PR cycle time on payments service",
    "Wire Prometheus scrape endpoint for FastAPI",
    "Set up automated RDS snapshot verification",
    "Fix sprint burndown calculation off-by-one",
    "Configure CloudFront cache invalidation for index.html",
]


def _iso_z(dt: datetime) -> str:
    return dt.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _sign_github(raw_body: bytes, secret: str) -> str:
    digest = hmac.new(secret.encode("utf-8"), raw_body, hashlib.sha256).hexdigest()
    return f"sha256={digest}"


def _day_multiplier(day_index: int, day_dt: datetime, rng: random.Random) -> float:
    mult = 0.25 if day_dt.weekday() >= 5 else 1.0
    if day_index % 14 in (12, 13):  # Sprint-end spike
        mult *= 1.8
    if day_dt.weekday() < 5 and rng.random() < 0.06:  # Occasional burst deploy
        mult *= 2.2
    return mult


def _parse_volume(value: str) -> int:
    if value in VOLUME_PER_DAY:
        return VOLUME_PER_DAY[value]
    parsed = int(value)
    if parsed < 1:
        raise argparse.ArgumentTypeError("--volume must be low, medium, high, or >= 1")
    return parsed


def generate_events(
    *,
    days: int,
    teams: int,
    repos: int,
    base_volume: int,
    seed: int = 42,
) -> list[tuple[str, str, str, dict]]:
    rng = random.Random(seed)
    now = datetime.now(UTC)
    repo_names = [f"acme/service-{i + 1}" for i in range(repos)]
    projects = [f"TEAM{t + 1}" for t in range(teams)]
    events: list[tuple[str, str, str, dict]] = []
    pr_seq = 1000
    ticket_seq = 100

    for day_offset in range(days - 1, -1, -1):
        day_dt = now - timedelta(days=day_offset)
        count = max(1, round(base_volume * _day_multiplier(days - day_offset, day_dt, rng)))

        for _ in range(count):
            event_time = day_dt.replace(
                hour=rng.randint(8, 19),
                minute=rng.randint(0, 59),
                second=rng.randint(0, 59),
                microsecond=0,
            )
            author = rng.choice(AUTHORS)
            repo = rng.choice(repo_names)
            roll = rng.random()

            if roll < 0.40:
                pr_seq += 1
                is_stalled = day_offset >= 4 and rng.random() < 0.12
                is_merged = not is_stalled and rng.random() < 0.80
                merged_at = (
                    min(now, event_time + timedelta(hours=rng.uniform(1.5, 36.0)))
                    if is_merged
                    else None
                )
                payload = {
                    "action": "closed" if merged_at else "opened",
                    "repository": {"full_name": repo},
                    "pull_request": {
                        "id": pr_seq,
                        "title": f"{rng.choice(PR_TITLES)} (#{pr_seq})",
                        "state": "closed" if merged_at else "open",
                        "user": {"login": author},
                        "created_at": _iso_z(event_time),
                        "merged_at": _iso_z(merged_at) if merged_at else None,
                    },
                }
                events.append(("github", "pull_request", str(uuid.uuid4()), payload))
            elif roll < 0.72:
                sha = uuid.uuid4().hex
                payload = {
                    "ref": "refs/heads/main",
                    "repository": {"full_name": repo},
                    "commits": [
                        {
                            "id": sha,
                            "message": rng.choice(COMMIT_MESSAGES),
                            "timestamp": _iso_z(event_time),
                            "author": {"username": author},
                        }
                    ],
                }
                events.append(("github", "push", str(uuid.uuid4()), payload))
            else:
                ticket_seq += 1
                project = rng.choice(projects)
                resolved = rng.random() < 0.72
                resolved_at = (
                    min(now, event_time + timedelta(hours=rng.uniform(4.0, 96.0)))
                    if resolved
                    else None
                )
                event_id = str(uuid.uuid4())
                from_status = "In Progress" if resolved else "To Do"
                to_status = "Done" if resolved else "In Progress"
                payload = {
                    "webhookEvent": "jira:issue_updated" if resolved else "jira:issue_created",
                    "webhookEventId": event_id,
                    "timestamp": int((resolved_at or event_time).timestamp() * 1000),
                    "issue": {
                        "key": f"{project}-{ticket_seq}",
                        "fields": {
                            "project": {"key": project},
                            "summary": f"{rng.choice(JIRA_SUMMARIES)} [{project}-{ticket_seq}]",
                            "status": {"name": to_status},
                            "assignee": {"displayName": author},
                            "created": _iso_z(event_time),
                            "resolutiondate": _iso_z(resolved_at) if resolved_at else None,
                        },
                    },
                    "changelog": {
                        "items": [
                            {
                                "field": "status",
                                "fromString": from_status,
                                "toString": to_status,
                            }
                        ]
                    },
                }
                events.append(("jira", payload["webhookEvent"], event_id, payload))

    return events


async def post_events(
    *,
    events: list[tuple[str, str, str, dict]],
    api_url: str,
    github_secret: str,
    jira_secret: str,
) -> dict[str, int]:
    sent = {"github": 0, "jira": 0}
    async with httpx.AsyncClient(base_url=api_url, timeout=15.0) as client:
        for source, event_type, event_id, payload in events:
            raw_body = json.dumps(payload).encode("utf-8")
            if source == "github":
                path = "/webhooks/github"
                headers = {
                    "Content-Type": "application/json",
                    "X-Hub-Signature-256": _sign_github(raw_body, github_secret),
                    "X-GitHub-Delivery": event_id,
                    "X-GitHub-Event": event_type,
                }
            else:
                path = "/webhooks/jira"
                headers = {
                    "Content-Type": "application/json",
                    "X-Jira-Webhook-Secret": jira_secret,
                    "X-Jira-Delivery": event_id,
                }

            while True:
                resp = await client.post(path, content=raw_body, headers=headers)
                if resp.status_code == 429:
                    wait_s = int(resp.headers.get("Retry-After", "2"))
                    await asyncio.sleep(max(wait_s, 1))
                    continue
                resp.raise_for_status()
                sent[source] += 1
                break
    return sent


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Seed realistic multi-day engineering events through the webhook endpoints.",
        epilog=(
            "When to use:\n"
            "  scripts/seed.py       -> full integration testing, 90-day demo data "
            "(via webhook endpoint)\n"
            "  scripts/seed_kafka.py -> consumer development/debugging "
            "(bypasses webhook, S3, idempotency)"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--days", type=int, default=90, help="Days of history to generate (default: 90)")
    parser.add_argument("--teams", type=int, default=2, help="Number of Jira projects/teams (default: 2)")
    parser.add_argument("--repos", type=int, default=3, help="Number of GitHub repositories (default: 3)")
    parser.add_argument(
        "--volume",
        type=_parse_volume,
        default="medium",
        help="Daily event volume: low (4), medium (10), high (24), or integer >= 1",
    )
    parser.add_argument(
        "--api-url",
        default=os.getenv("VITE_API_URL", "http://localhost:8000"),
        help="FastAPI base URL (default: http://localhost:8000)",
    )
    parser.add_argument(
        "--github-secret",
        default=os.getenv("GITHUB_WEBHOOK_SECRET", ""),
        help="GitHub webhook secret (defaults to GITHUB_WEBHOOK_SECRET env var)",
    )
    parser.add_argument(
        "--jira-secret",
        default=os.getenv("JIRA_WEBHOOK_SECRET", ""),
        help="Jira webhook secret (defaults to JIRA_WEBHOOK_SECRET env var)",
    )
    parser.add_argument("--seed", type=int, default=42, help="PRNG seed for reproducibility")
    args = parser.parse_args()
    if args.days < 1 or args.teams < 1 or args.repos < 1:
        parser.error("--days, --teams, and --repos must be >= 1")
    if not args.github_secret or not args.jira_secret:
        parser.error("GITHUB_WEBHOOK_SECRET and JIRA_WEBHOOK_SECRET must be set via env or flags")
    return args


def main() -> None:
    args = parse_args()
    events = generate_events(
        days=args.days,
        teams=args.teams,
        repos=args.repos,
        base_volume=args.volume,
        seed=args.seed,
    )
    counts = asyncio.run(
        post_events(
            events=events,
            api_url=args.api_url,
            github_secret=args.github_secret,
            jira_secret=args.jira_secret,
        )
    )
    print(
        f"Seeded {counts['github']} GitHub + {counts['jira']} Jira events "
        f"across {args.days} day(s) via {args.api_url}"
    )


if __name__ == "__main__":
    main()
