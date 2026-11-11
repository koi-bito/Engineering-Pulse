# Engineering Pulse — Project Plan

> A Real-Time Dev Workflow Aggregator · Multi-Tenant · Anomaly-Detecting · Production-Grade

---

## Overview

Engineering Pulse ingests GitHub and Jira events via webhooks, streams them through Kafka, processes and stores them across PostgreSQL / Redis / Elasticsearch, and surfaces team-level metrics on a live React dashboard — all deployed on AWS with Terraform.

The project is built in two halves and three extension phases:

- **Phase 1** — Core pipeline: ingestion → processing → storage → API → dashboard → AWS deploy
- **Phase 2** — Production hardening: observability, CI/CD, documentation
- **Phase 3** — Multi-team & multi-org support with tenant isolation
- **Phase 4** — Anomaly detection & workflow degradation alerting
- **Phase 5** — Elasticsearch relevance tuning & advanced search

**Stack at a glance:**
FastAPI · Kafka · PostgreSQL · Redis · Elasticsearch · React · S3 · CloudFront · ALB · EC2 · IAM · Terraform · Prometheus · Grafana · Docker Compose
_(Prophet: optional extension — see Phase 4)_

---

## Project File Structure (Full)

Every file below should exist in the repo by the end of the project.

```
engineering-pulse/
├── backend/
├── frontend/
├── infra/
├── scripts/
│   └── seed.py               # generates 90 days of realistic fake events
├── docker-compose.yml        # LOCAL dev: all services including Postgres + LocalStack
├── docker-compose.prod.yml   # PRODUCTION: excludes Postgres (uses RDS), LocalStack, and ES (Phase 1–4)
├── .env.example              # all required env vars with descriptions — no values
├── README.md
├── ARCHITECTURE.md
├── DECISIONS.md
├── CHANGELOG.md
├── RUNBOOK.md
├── TESTING.md
├── PERFORMANCE.md
├── AI_BUILD_LOG.md
└── CONTRIBUTING.md
```

> **Local vs prod docker-compose — the rule:**
>
> - `docker-compose.yml` — runs everything locally including Postgres. Used for development and testing.
> - `docker-compose.prod.yml` — excludes Postgres (points to RDS), excludes LocalStack (uses real S3), excludes ES during Phase 1–4 (deferred to save RAM). Used on EC2.
>   This distinction must be explicit — otherwise you will accidentally run Postgres on EC2 alongside RDS, or run LocalStack in production.

> **`.env.example` must be complete.** Every variable the app reads must have a commented entry. Forgetting one variable means 30 minutes of "why is this None?" debugging. Write `.env.example` as you add each new env var — not at the end.

### File Ownership by Phase

| File              | Created         | Actively Updated                   |
| ----------------- | --------------- | ---------------------------------- |
| `README.md`       | Phase 0         | Phase 2, after each major phase    |
| `ARCHITECTURE.md` | Phase 0 (stub)  | Every phase                        |
| `DECISIONS.md`    | Phase 0 (stub)  | Every time a tech decision is made |
| `AI_BUILD_LOG.md` | Phase 0         | Every session                      |
| `CHANGELOG.md`    | Phase 1         | Every phase completion             |
| `RUNBOOK.md`      | Phase 1.5 (AWS) | Phase 2, Phase 3+                  |
| `TESTING.md`      | Phase 2         | Phase 3, 4, 5                      |
| `PERFORMANCE.md`  | Phase 2         | After any optimization             |
| `CONTRIBUTING.md` | Phase 2         | As setup steps change              |

---

## How to Use AI_BUILD_LOG.md

Log every meaningful AI interaction while building. Format per entry:

```markdown
## 2026-11-07 · Phase 1.1 — Kafka Producer Setup

**Problem I was solving:** Needed async Kafka producer that doesn't block the webhook receiver

**What I asked AI:** How to initialize aiokafka AsyncProducer in FastAPI lifespan

**What AI suggested:** Use FastAPI lifespan context manager, start producer on startup, stop on shutdown

**What I did:** Accepted the pattern but modified to add retry logic on startup failure — AI's version assumed Kafka was always up

**What I learned:** FastAPI lifespan replaces the old @app.on_event pattern. Producer startup failure silently swallows errors without explicit try/except.

**Decision logged in DECISIONS.md:** Yes — why aiokafka over confluent-kafka-python
```

This file is the most differentiating thing in your repo. It shows intellectual ownership, not just code output.

---

## How to Use DECISIONS.md (ADR Format)

```markdown
## ADR-001 — Use Kafka over direct async queue

**Date:** 2026-11-07
**Status:** Accepted

**Context:** Webhook receiver needs to hand off events without blocking. Options: direct DB write, async task queue (Celery/ARQ), SQS, Redis Streams, Kafka.

**Decision:** Kafka

**Reasons:**

- Decouples ingestion from processing — receiver stays fast under burst traffic
- Consumer group model allows horizontal scaling without changing producer code
- Built-in replay via offset reset — critical for recovering from consumer bugs: reset the offset, re-run the consumer, the events are all still there
- S3 raw archive + Kafka offset = two independent replay mechanisms

**Trade-offs accepted:**

- Operational overhead (Zookeeper, topic management) not justified for small teams
- Adds ~50ms latency vs direct DB write
- On EC2: single broker, no replication — acceptable for portfolio, not production (production would use MSK)

**Rejected alternatives:**

- SQS: no consumer-group replay — once a message is consumed and deleted, it's gone. The replay story is the core reason Kafka wins here.
- Redis Streams: closer to Kafka than SQS, has consumer groups and replay, but less operational tooling and ecosystem depth. Valid alternative — chose Kafka for wider JD recognition and richer observability (consumer lag metrics).
- Celery/ARQ: adds Redis or RabbitMQ as broker anyway, no replay capability, designed for task queues not event streams
- Direct DB write: couples receiver to DB, blocks under load, no replay
```

> **Interview note:** When someone asks "why not SQS?" or "why not Redis Streams?", your real answer is the **consumer-group replay story**. SQS deletes messages on consumption — you cannot reset and reprocess. Kafka lets you reset the consumer offset to any point and replay the entire event history. Demo this in your interview if you can — reset the offset, re-run the consumer against the seeded data, show the metrics recalculate correctly.

---

---

# PHASE 0 — Foundations & Local Environment

> Do this once, do it right. Everything else depends on it.

---

## Tasks

### 0.1 Repository Setup

- [ ] Create GitHub repo: `engineering-pulse`
- [ ] Monorepo structure:
  ```
  engineering-pulse/
  ├── backend/
  ├── frontend/
  ├── infra/
  ├── docker-compose.yml
  ├── .env.example
  └── README.md
  ```
- [ ] Add `.gitignore` (Python, Node, Terraform, `.env`)
- [ ] Create stubs: `ARCHITECTURE.md`, `DECISIONS.md`, `AI_BUILD_LOG.md` — headers only, no content yet

### 0.2 Docker Compose (Local Dev)

- [ ] Services:
  - `kafka` (Confluent KRaft mode — no Zookeeper, port 9092)
  - `postgres` (port 5432)
  - `redis` (port 6379)
  - `localstack` (port 4566 — local S3 emulation, no real AWS needed in dev)
  - `elasticsearch` (port 9200, single-node)
  - `prometheus` (port 9090)
  - `grafana` (port 3000)
- [ ] **JVM heap caps** — prevent Kafka and ES from claiming all available RAM on the t3.large:
  - Kafka: `KAFKA_HEAP_OPTS: "-Xmx512m -Xms512m"` (Confluent image defaults to 1GB — overkill for single-broker dev/portfolio)
  - Elasticsearch: `ES_JAVA_OPTS: "-Xmx512m -Xms512m"` (default is half of system RAM — will OOM-kill other containers)
  - Without these, Kafka + ES alone consume 3–4GB of your 8GB t3.large, leaving insufficient RAM for Postgres, Redis, Prometheus, Grafana, and your app
- [ ] Verify all start cleanly: `docker compose up -d`
- [ ] Add health checks for each service

### 0.2b Kafka Topic Creation

- [ ] Add a one-time init container in `docker-compose.yml` that creates topics before any producer or consumer starts:
  ```yaml
  kafka-init:
    image: confluentinc/cp-kafka:7.6.0
    depends_on:
      kafka:
        condition: service_healthy
    entrypoint: ["/bin/sh", "-c"]
    command: |
      "kafka-topics --bootstrap-server kafka:9092 --create --if-not-exists --topic github-events --partitions 3 --replication-factor 1 &&
       kafka-topics --bootstrap-server kafka:9092 --create --if-not-exists --topic jira-events --partitions 3 --replication-factor 1 &&
       kafka-topics --bootstrap-server kafka:9092 --create --if-not-exists --topic events-dlq --partitions 1 --replication-factor 1"
  ```
- [ ] Set `KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"` on the broker — prevents accidental topics with wrong partition counts from typos
- [ ] For production (EC2): same topic creation runs once via a deploy script, not auto.create

> **Why KRaft, not Zookeeper:** Confluent dropped Zookeeper dependency in KRaft mode (KIP-500, GA since Kafka 3.3). Zookeeper adds ~500MB RAM on your already-tight EC2 and is one more thing to crash. KRaft is the modern default — use `confluentinc/cp-kafka` with `KAFKA_PROCESS_ROLES: broker,controller` and `KAFKA_NODE_ID: 1`. Log in `DECISIONS.md` as ADR.
>
> **Why LocalStack:** Lets you test S3 writes locally without an AWS account or cost. Set `AWS_ENDPOINT_URL=http://localhost:4566` in your `.env`. Same boto3 code works against both LocalStack and real S3 — just swap the endpoint. Remove from `docker-compose.prod.yml`.

- [ ] `DECISIONS.md` — ADR: KRaft over Zookeeper (memory, simplicity, modern default)

### 0.3 Backend Scaffold

- [ ] Python project with `pyproject.toml`
- [ ] Dependencies: `fastapi`, `uvicorn`, `aiokafka`, `asyncpg`, `redis[asyncio]`, `elasticsearch[async]`, `prometheus-fastapi-instrumentator`, `pydantic`, `python-dotenv`, `alembic`, `boto3`, `apscheduler`, `slowapi`, `structlog`
  - `structlog`: structured JSON logging with `event_id` correlation across every pipeline step — when a consumer fails, you can trace exactly which event caused it without grepping unstructured logs
- [ ] Dev dependencies: `pytest`, `pytest-asyncio`, `httpx`, `ruff`, `locust`
- [ ] Structure:
  ```
  backend/
  ├── app/
  │   ├── main.py
  │   ├── config.py
  │   ├── routers/
  │   ├── consumers/
  │   ├── models/
  │   ├── schemas/
  │   ├── services/
  │   └── db/
  └── Dockerfile
  ```
- [ ] `config.py` — pydantic BaseSettings, all secrets from environment only
  - Must include `DATABASE_URL` (local: `postgresql+asyncpg://postgres:postgres@localhost:5432/engineering_pulse`, prod: RDS endpoint from Secrets Manager)
  - `docker-compose.yml` sets `DATABASE_URL` pointing to the local `postgres` service
  - `docker-compose.prod.yml` sets `DATABASE_URL` pointing to the RDS endpoint
  - This is the single source of truth for which database the app connects to — not derived from `ENVIRONMENT`

### 0.4 Frontend Scaffold

- [ ] Vite + React + TypeScript
- [ ] Dependencies: `recharts`, `axios`, `@tanstack/react-query` (not `react-query` — package was renamed in v5)
- [ ] Add `VITE_API_URL` to `.env.example` — local: `http://localhost:8000`, prod: ALB/CloudFront API domain. All `axios` calls use `import.meta.env.VITE_API_URL` as base URL. Without this, the frontend has a hardcoded `localhost:8000` that silently fails in production.
- [ ] Basic shell: Header, Sidebar, Main content area
- [ ] Stub: `useWebSocket.ts`

### 0.5 CORS Configuration (do this in Phase 0, not when it breaks)

- [ ] Add `fastapi.middleware.cors.CORSMiddleware` to `main.py`
- [ ] Allowed origins:
  - Local dev: `http://localhost:5173`
  - Production: your CloudFront domain (e.g. `https://d1234abcd.cloudfront.net`) — add before deploy
- [ ] Allowed methods: `GET`, `POST`, `PUT`, `DELETE`, `OPTIONS`
- [ ] Allowed headers: `Authorization`, `Content-Type`
- [ ] `allow_credentials: True` — required for the httpOnly refresh cookie to be sent cross-origin
- [ ] `DECISIONS.md` — ADR: why specific origins instead of `*` (credentials cannot be sent with wildcard origin)

> **Why Phase 0:** The first time you open your React app and call the FastAPI backend, the browser will block the request with a CORS error. This is not a deployment problem — it happens in local dev too. Configuring it once here means you never debug it under pressure later.

### 0.6 Environment Parity

- [ ] Add `ENVIRONMENT` env var to `.env.example`: values `local` or `production`
- [ ] `config.py` reads `ENVIRONMENT` and adjusts behavior:
  - `local`: use LocalStack endpoint for S3, verbose logging, relaxed CORS
  - `production`: use real AWS endpoints, structured JSON logging only, strict CORS
- [ ] This is the single switch that controls local vs prod behavior — no more "why does this work locally but not on EC2?"
- [ ] `DECISIONS.md` — ADR: single ENVIRONMENT flag vs per-service feature flags (simplicity)

### 0.7 Timezone Policy

- [ ] **All timestamps stored as UTC** — `TIMESTAMP WITH TIME ZONE` in Postgres, ISO 8601 with `Z` suffix in Kafka messages and API responses
- [ ] GitHub sends UTC natively. Jira sends whatever the instance is configured for — normalize to UTC on ingestion in the consumer, not at query time
- [ ] Frontend converts UTC to the user's local timezone for display only — never store local time
- [ ] Add to `.env.example`: no timezone config needed — UTC is the default, not a setting
- [ ] `DECISIONS.md` — ADR: UTC everywhere, frontend converts for display (why not store local time)

### 0.8 Local Development Workflow

> Decide how you'll work day-to-day before writing pipeline code. Saves hours of "how do I..." interruptions.

- [ ] **Backend:** run FastAPI outside Docker (`uvicorn app.main:app --reload`) pointed at Dockerized infrastructure via `.env`. Faster iteration than rebuilding the container on every change.
- [ ] **Consumers:** run outside Docker during development (`python -m app.consumers.github_consumer`), inside Docker for integration testing
- [ ] **Frontend:** run Vite dev server outside Docker (`npm run dev`) — HMR doesn't work well inside containers
- [ ] **Useful commands to document in `CONTRIBUTING.md`:**
  - `docker compose logs -f github-consumer` — tail a specific consumer
  - `docker compose exec postgres psql -U postgres engineering_pulse` — connect to local DB
  - `docker compose down -v && docker compose up -d` — full reset (wipes volumes)
  - `pytest tests/unit/ -x -v` — run one test file, stop on first failure

## Documentation Tasks (Phase 0)

- [ ] `DECISIONS.md` — log ADR-001: why monorepo structure
- [ ] `AI_BUILD_LOG.md` — log first entry: scaffold decisions made with AI assistance
- [ ] `ARCHITECTURE.md` — write the data flow diagram stub (even if incomplete)

## ✅ Eval: Phase 0 is Done When

- `docker compose up -d` starts all 7 services with no errors
- FastAPI app runs locally at `localhost:8000`
- React app runs at `localhost:5173`
- All three stub docs exist in repo with at least headers

---

---

# PHASE 1 — Core Build (First 50%)

> Goal: fully working pipeline from webhook to live dashboard, deployed on AWS.

---

## Learning Checkpoints

> Understand these before writing a single line of Phase 1 code.

- [ ] **Kafka**: What is a consumer group? Why do offsets matter? What is at-least-once delivery and what problem does it solve?
- [ ] **WebSockets vs polling**: Why does the dashboard use WebSockets instead of polling the API every second?
- [ ] **Redis Pub/Sub vs Redis cache**: These are two different Redis features used for two different things in this project. Can you explain both?
- [ ] **Why S3 before Kafka**: What happens if Kafka goes down for 10 minutes? What does the S3 archive let you do that you couldn't do otherwise?
- [ ] **Async FastAPI**: What does `async def` actually do in FastAPI? What would break if you used `def` instead on the webhook receiver?

---

## 1.0b — Testing Foundation (do this before writing pipeline code)

> By Phase 2 you will have ~3,000 lines of pipeline code — webhook validation, Kafka producer/consumer, Pydantic normalization, DB writes, Redis caching, WebSocket fan-out. Without tests in place during Phase 1, you will spend Phase 2 debugging instead of hardening. These are not comprehensive tests — they are the minimum that catch real bugs early.

- [ ] Add to dependencies: `pytest`, `pytest-asyncio`, `httpx` (async test client for FastAPI)
- [ ] `pytest.ini` or `pyproject.toml` test config: `asyncio_mode = auto`
- [ ] `tests/` directory structure:
  ```
  tests/
  ├── unit/
  │   ├── test_normalization.py    # Pydantic model parsing
  │   └── test_idempotency.py      # Redis dedup logic
  ├── integration/
  │   └── test_webhook_receiver.py # POST /webhooks/github → Kafka message shape
  └── conftest.py                  # shared fixtures: test client, mock Redis, mock Kafka
  ```
- [ ] **Unit: event normalization** — these are pure functions, highest ROI tests in the project
  - Valid GitHub PR payload → `PullRequestEvent` with correct fields
  - Missing required field → Pydantic `ValidationError` raised, not silently ignored
  - `cycle_time_hours` calculated correctly from `opened_at` and `merged_at`
- [ ] **Unit: idempotency logic**
  - First call with an `event_id` → returns `False` (not seen), sets Redis key
  - Second call with same `event_id` → returns `True` (already seen), no Kafka publish
  - This is a correctness requirement, not a feature — test it
- [ ] **Integration: webhook receiver**
  - Mock Kafka producer and Redis — don't need real services for this
  - `POST /webhooks/github` with valid payload + valid signature → 202, Kafka `send` called once
  - `POST /webhooks/github` with invalid signature → 401, Kafka `send` never called
  - `POST /webhooks/github` with duplicate `event_id` → 202, Kafka `send` never called (dedup working)
- [ ] `DECISIONS.md` — ADR: what is tested in unit vs integration vs not at all (and why)
- [ ] Add `pytest` to `ci.yml` — tests run on every push from Phase 1 onward

---

## 1.1 — Webhook Ingestion

### Webhook Receiver

- [ ] `POST /webhooks/github` — receives GitHub webhook payloads
- [ ] `POST /webhooks/jira` — receives Jira webhook payloads
- [ ] Validate webhook signatures (`X-Hub-Signature-256` for GitHub, secret token for Jira)
- [ ] Return `202 Accepted` immediately — no blocking processing

### S3 Raw Payload Archive

- [ ] Write raw JSON body to S3 on every webhook hit
  - Key pattern: `raw/{source}/{YYYY}/{MM}/{DD}/{event_id}.json`
- [ ] Bucket: `engineering-pulse-raw-events`
- [ ] Use `boto3` in a threadpool (non-blocking)

### Kafka Publishing

- [ ] Topics: `github-events`, `jira-events` (3 partitions each)
- [ ] Message format:
  ```json
  {
    "event_id": "...",
    "source": "github",
    "type": "pull_request",
    "received_at": "...",
    "payload": {}
  }
  ```
- [ ] `aiokafka` AsyncProducer initialized at app startup via lifespan

### Idempotency & Rate Limiting

- [ ] **Idempotency:** before publishing to Kafka, check `event_id` against Redis key `seen:events:{event_id}` (TTL: 24h)
  - If key exists → skip publish, return 202
  - If not → set key, write to S3, publish to Kafka
  - Handles GitHub retries on timeout — without this, one delivery becomes duplicate events polluting every downstream metric
- [ ] **Rate limiting:** add `slowapi` (rate limiting library for FastAPI) — limit per webhook secret, not per IP
  - **Storage backend: Redis** — configure `slowapi` with `redis.asyncio` as the backing store (`from slowapi.util import get_remote_address`; custom key function extracts webhook secret). Default is in-memory, which resets on restart and doesn't share state across instances.
  - Limit: 100 requests/minute per webhook secret (configurable via env var)
  - On exceeded: return 429 with `Retry-After` header
  - Why per-secret not per-IP: GitHub and Jira webhook sources have fixed IPs that may be shared — secret is the correct identity boundary
  - Idempotency alone is not rate limiting — a flood of unique events still overwhelms Kafka without it
- [ ] `DECISIONS.md` — ADR: idempotency at receiver vs consumer (receiver is correct — deduplicate before the pipeline)
- [ ] `DECISIONS.md` — ADR: rate limit per webhook secret vs per IP

### Seed Script — Pass 1 (build here, in 1.1)

- [ ] `scripts/seed_kafka.py` — writes test messages directly to Kafka topics, bypassing the webhook receiver
  - Accepts: `--count`, `--topic` (`github-events` or `jira-events`), `--type` (pr, commit, ticket)
  - Generates one realistic message per event type with all required fields
  - Use this to develop and test consumers independently — you do not need a working webhook to test a consumer
- [ ] Why pass 1 here: consumers and seed script have a circular dependency if built together — you need data to test consumers, but the data generator depends on knowing what consumers expect. Break the cycle by having a minimal direct-to-Kafka seed first.
- [ ] **Lifecycle:** Pass 1 remains useful even after Pass 2 exists — use it to test consumers in isolation without running the full webhook stack. Keep both scripts, document in each script's `--help` when to use which:
  - `seed_kafka.py` — consumer development and debugging (bypasses webhook, S3, idempotency)
  - `seed.py` (Pass 2) — full integration testing, demo data, screenshots

### End-to-End Ingestion Test

- [ ] Fire real events via `ngrok` or GitHub webhook delivery test
- [ ] Confirm messages in Kafka via `kafka-console-consumer`
- [ ] Confirm raw JSON in S3 (LocalStack locally)
- [ ] Deliberately re-send the same event — confirm it is deduplicated and does not appear twice in Kafka

### Documentation Tasks

- [ ] `DECISIONS.md` — ADR: why aiokafka over confluent-kafka-python
- [ ] `DECISIONS.md` — ADR: why S3 archive before Kafka publish (not after)
- [ ] `AI_BUILD_LOG.md` — log webhook signature validation approach
- [ ] `ARCHITECTURE.md` — fill in ingestion layer section

---

## 1.2 — Kafka Consumers & Storage

### Consumer Deployment Model (decide this now — it affects docker-compose structure)

- [ ] **Decision: separate Docker service per consumer** — not threads inside FastAPI, not subprocesses
  - `docker-compose.yml` has `github-consumer` and `jira-consumer` as independent services
  - Each runs `python -m app.consumers.github_consumer` as its entrypoint
  - A consumer crash does not take down the API — they are completely independent processes
  - Each can be restarted, scaled, or debugged independently
  - `DECISIONS.md` — ADR: consumer as separate Docker service vs thread inside FastAPI (coupling, independent restart, crash isolation)

### Consumer Startup Ordering (migration safety)

- [ ] **Problem:** `docker compose up` starts all services in parallel. If a consumer starts before Alembic migrations have run, it will hit missing columns and crash.
- [ ] **Solution:** add a `wait-for-migrations.sh` entrypoint script for each consumer:
  - Polls the Alembic `alembic_version` table until the expected revision is present (or the API health check passes)
  - Falls back to a simple 10-second sleep if `alembic_version` doesn't exist yet (first run)
- [ ] Alternative: add `depends_on` with `condition: service_healthy` on the API service (which runs migrations during lifespan startup), so consumers only start after the API is healthy
- [ ] Choose one approach — both work. The `depends_on` approach is simpler but couples consumer startup to the API.

### Consumer Architecture

- [ ] `consumers/github_consumer.py`, `consumers/jira_consumer.py`
- [ ] Consumer group: `engineering-pulse-consumers`
- [ ] **Set `enable_auto_commit=False`** on all consumers — `aiokafka` defaults to `True` (auto-commits every 5s regardless of write success). Without this, your "commit after write" strategy silently does nothing and you get at-most-once delivery instead of at-least-once.
- [ ] Commit offsets only after successful write (manual `await consumer.commit()`)
- [ ] Handle rebalancing gracefully

### Event Normalization

- [ ] Pydantic models:
  - `PullRequestEvent`: id, repo, author, title, state, opened_at, merged_at, cycle_time_hours
  - `CommitEvent`: id, repo, author, message, sha, timestamp
  - `JiraTicketEvent`: id, project, assignee, summary, status, created_at, resolved_at, cycle_time_hours
  - `JiraTransitionEvent`: ticket_id, from_status, to_status, transitioned_at

### PostgreSQL Schema

- [ ] Tables: `pull_requests`, `commits`, `jira_tickets`, `jira_transitions`
- [ ] Alembic migrations
- [ ] Indexes on: `repo`, `author`, `opened_at`, `merged_at`, `status`

### Redis Caching

- [ ] Cache keys: `metrics:prs_merged_today`, `metrics:avg_cycle_time_7d`, `sprint:active:{project_key}`
- [ ] TTL: 5 min metrics, 1 min sprint
- [ ] API reads Redis first, falls back to Postgres

### Elasticsearch Indexing

- [ ] Index: `engineering-events`
- [ ] Explicit field type mapping (keyword vs text vs date)
- [ ] Postgres is source of truth — ES written after Postgres
- [ ] **Feature flag:** guard ES writes behind `ES_ENABLED` env var (default `true` locally, `false` in `docker-compose.prod.yml` during Phases 1–4). Without this, consumers crash or silently fail in prod when ES is excluded from the production compose file.

### Redis Pub/Sub Publishing (Consumer → Dashboard)

- [ ] After writing to Postgres (and ES if enabled), each consumer publishes a summary to Redis Pub/Sub channel `live-feed`
- [ ] Consumers need their own Redis connection config — add `REDIS_URL` to consumer environment in `docker-compose.yml`
- [ ] Message format: `{ "type": "event", "source": "github|jira", "event_type": "pull_request|commit|ticket", "summary": "...", "timestamp": "..." }`
- [ ] This is the bridge between consumer containers and the WS handler in the API container

### Dead Letter Queue

- [ ] On consumer failure → publish to `events-dlq`
- [ ] DLQ consumer → `failed_events` Postgres table with error + raw payload

### PostgreSQL Connection Pooling

- [ ] Each service creates its own `asyncpg.create_pool()` — API, github-consumer, and jira-consumer are **separate Docker containers** and cannot share an in-process pool
  - API: `min_size=5, max_size=10`
  - Each consumer: `min_size=2, max_size=5`
  - Total worst-case: 10 + 5 + 5 = 20 connections — well within `db.t3.micro` max of 87
- [ ] Pool initialized at service startup (FastAPI lifespan for API, script entrypoint for consumers)
- [ ] Why: `db.t3.micro` has a default max of 87 connections. Without pooling, each request/event opens a new connection — under burst traffic, connections exhaust within seconds.
- [ ] Add Prometheus gauge per service: `postgres_pool_active_connections{service="api|github-consumer|jira-consumer"}`
- [ ] `DECISIONS.md` — ADR: per-service pool sizing (why not shared — separate containers), max connection budget across all services

### Kafka Producer Retry Config

- [ ] Configure producer with: `request_timeout_ms=30000`, `retry_backoff_ms=500`, `max_in_flight_requests_per_connection=1` (preserves ordering on retry)
- [ ] On final failure after retries: log error with `event_id` and raw payload — the event is still in S3, so it can be replayed manually. This is exactly why S3-before-Kafka is the right design.
- [ ] `DECISIONS.md` — ADR: why producer retry + S3 archive is preferable to exactly-once delivery (simplicity vs correctness trade-off)

### Seed / Demo Script — Pass 2 (full 90-day generator)

- [ ] `scripts/seed.py` — generates 90 days of realistic fake events and loads them via the webhook endpoint
- [ ] Generates: pull requests (opened, merged, stalled), commits, Jira tickets (transitions, resolutions), sprint cycles
- [ ] Realistic distributions: less activity on weekends, sprint-end spikes, occasional burst deploys
- [ ] Accepts flags: `--days`, `--teams`, `--repos`, `--volume`
- [ ] This script is needed for: Phase 4 anomaly detection testing, Phase 5 search quality tests, screenshots, demos, and interview prep
- [ ] Log in `AI_BUILD_LOG.md` — data shape decisions are worth recording

### Documentation Tasks

- [ ] `DECISIONS.md` — ADR: why offset commit after write, not before
- [ ] `DECISIONS.md` — ADR: why Elasticsearch is secondary to Postgres
- [ ] `AI_BUILD_LOG.md` — log DLQ pattern decisions
- [ ] `ARCHITECTURE.md` — fill consumer + storage layer section

---

## 1.3 — API & WebSocket Layer

### REST Endpoints

- [ ] `GET /api/metrics/summary`
- [ ] `GET /api/metrics/prs?range=7d`
- [ ] `GET /api/metrics/cycle-time?range=30d`
- [ ] `GET /api/sprint/active`
- [ ] `GET /api/search?q={query}`
- [ ] `GET /health`
- [ ] All read Redis first

### Consistent Error Response Format

- [ ] Define a standard error body used by all endpoints:
  ```json
  {
    "error": "rate_limit_exceeded",
    "detail": "100 requests/min exceeded",
    "retry_after": 60
  }
  ```
- [ ] Use FastAPI exception handlers to enforce this shape for 400, 401, 404, 422, 429, 500
- [ ] Frontend `axios` interceptor reads this structure for user-facing error messages

### WebSocket Real-Time Feed

- [ ] `WS /ws/feed`
- [ ] **Define message protocol now** — every WS message has a `type` field for client-side routing:
  ```json
  { "type": "event", "source": "github", "event_type": "pull_request", "data": {...} }
  { "type": "alert", "severity": "critical", "data": {...} }
  { "type": "auth", "token": "<access_token>" }
  ```
  Phase 4 adds `"type": "alert"` messages — without a type discriminator from Phase 1, you'll need a protocol migration later.
- [ ] Flow: Kafka consumer → Redis Pub/Sub `live-feed` → WS handler → browser
- [ ] Subscribe on connect, unsubscribe on disconnect

### Prometheus Instrumentation

- [ ] `prometheus-fastapi-instrumentator`
- [ ] Custom metrics: `kafka_consumer_lag`, `webhook_events_received_total`, `cache_hit_ratio`
- [ ] Expose `/metrics`

### Documentation Tasks

- [ ] `DECISIONS.md` — ADR: why Redis Pub/Sub for WS fan-out instead of direct consumer→WS
- [ ] `AI_BUILD_LOG.md` — log WebSocket reconnect strategy
- [ ] `ARCHITECTURE.md` — fill API + WS layer section

---

## 1.4 — React Dashboard

### Core Panels

- [ ] PRs Merged / Day — BarChart, last 14 days
- [ ] Ticket Cycle Time — LineChart, 30-day rolling average
- [ ] Active Sprint Board — tickets by status
- [ ] Live Event Feed — WebSocket scrolling list

### Search Panel

- [ ] Search bar → `GET /api/search?q=`, debounced 300ms
- [ ] Results: type, source, timestamp, snippet

### WebSocket Integration

- [ ] `useWebSocket` hook — reconnect logic must refresh the access token before reconnecting:
  1. Connection drops → start exponential backoff timer
  2. Before reconnecting → call `/auth/jwt/refresh` to get a fresh access token
  3. If refresh succeeds → reconnect with new token as first message
  4. If refresh fails (refresh token expired) → redirect to login, do not loop
  - **Why:** access tokens expire in 15 minutes. If the WS connection drops after 15 minutes, the original token is expired. Without step 2, reconnect sends an expired token → server closes with 4401 → reconnect → 4401 → infinite loop. The user sees a broken live feed with no error message.
- [ ] New events prepended, capped at 50

### Polish

- [ ] Loading skeletons, error states
- [ ] Auto-refresh every 60s
- [ ] Responsive at 1280px

### Documentation Tasks

- [ ] `DECISIONS.md` — ADR: why react-query over raw useEffect for data fetching
- [ ] `AI_BUILD_LOG.md` — log any generated component code and what you changed

---

## 1.5 — AWS Infrastructure (Terraform)

### Terraform Structure

```
infra/
├── main.tf
├── variables.tf
├── outputs.tf
└── modules/
    ├── vpc/
    ├── ec2/
    ├── rds/
    ├── s3/
    ├── alb/
    └── cloudfront/
```

- [ ] Remote state: S3 bucket + DynamoDB lock table

### Networking

- [ ] VPC, public + private subnets (2 AZs)
- [ ] Internet Gateway, NAT Gateway
- [ ] Security groups: ALB (80/443 public), EC2 (8000 from ALB only), RDS (5432 from EC2 only)

### Compute

- [ ] EC2 `t3.large` (8GB RAM), Amazon Linux 2023
- [ ] IAM instance profile: S3 PutObject, rds-db:connect — no hardcoded keys
- [ ] User data: install Docker, pull repo, `docker compose up`

### Disk \u0026 Log Management

- [ ] **EBS volume:** size to at least 50GB (default 8GB will fill with Kafka logs, Prometheus TSDB, and app logs within weeks of seeded data)
- [ ] **Docker log rotation:** add to `/etc/docker/daemon.json`:
  ```json
  {
    "log-driver": "json-file",
    "log-opts": { "max-size": "50m", "max-file": "3" }
  }
  ```
- [ ] **Kafka log retention:** set `KAFKA_LOG_RETENTION_HOURS: 168` (7 days) and `KAFKA_LOG_RETENTION_BYTES: 1073741824` (1GB per partition) — whichever hits first
- [ ] **Prometheus retention:** already set to 15 days (Phase 2) — verify `--storage.tsdb.retention.size=5GB` is also set to prevent disk exhaustion

> **Why t3.large, not t3.medium:** Kafka + Zookeeper alone consume ~1.5GB. Add Elasticsearch (1GB heap minimum), PostgreSQL, Redis, Prometheus, Grafana, and your FastAPI app — you will exceed 4GB on a t3.medium and services will start crashing silently. t3.large (8GB) gives you comfortable headroom. Cost difference is ~$30/month — worth it to avoid mysterious OOM failures.
>
> **Elasticsearch on EC2 — Phase 1 option:** If you want to keep costs lower during Phase 1, run ES locally only and defer it to EC2 until Phase 5 (when you actually need it for search tuning). The pipeline writes to ES but the search endpoint won't be used until Phase 5. Add a `docker-compose.prod.yml` that excludes ES, and add it back for Phase 5. Document this in `DECISIONS.md`.

- [ ] `DECISIONS.md` — ADR: t3.large over t3.medium (memory budget for all services)

### Database

- [ ] RDS PostgreSQL `db.t3.micro`, private subnet
- [ ] 7-day backup retention
- [ ] **Backup validation:** after first deploy, perform a test restore from RDS snapshot to a temporary instance, verify data integrity, delete temporary instance. Add procedure to `RUNBOOK.md`. An untested backup is not a backup.
- [ ] Credentials in AWS Secrets Manager

### Redis State on EC2 (conscious decision — document it)

- [ ] **Redis has no backup.** If the EC2 instance dies:
  - Cached metrics → gone (rebuilt on next API query from Postgres — acceptable)
  - Rate-limit state → gone (resets to zero — acceptable)
  - Idempotency keys → gone (24h TTL — acceptable unless webhook sources replay events older than 24h, which GitHub does not)
- [ ] This is a conscious trade-off, not an oversight — document in `DECISIONS.md`
- [ ] Production upgrade path: ElastiCache Redis with Multi-AZ replication
- [ ] `DECISIONS.md` — ADR: ephemeral Redis accepted for portfolio scope (what is lost, why it's acceptable, production path)

### Load Balancer & Frontend

- [ ] ALB → EC2:8000, health check `GET /health`
- [ ] S3 static hosting + CloudFront distribution
- [ ] Cache invalidation on deploy: `aws cloudfront create-invalidation --paths "/index.html"` — invalidate only `index.html`, not `/*`
  - Vite already content-hashes all JS/CSS bundles (`main.a1b2c3.js`) — CloudFront can cache those forever, they never change between deploys
  - `index.html` is the only file that changes (it references the new bundle filenames)
  - `/*` invalidation costs $0.005/path after 1,000/month and takes 5–15 minutes — invalidating one file is instant and free

### Documentation Tasks

- [ ] `DECISIONS.md` — ADR: Kafka on EC2 vs MSK (cost vs ops trade-off)
- [ ] `DECISIONS.md` — ADR: why IAM instance profile instead of access keys
- [ ] `DECISIONS.md` — ADR: single EC2 instance as accepted SPOF for portfolio scope
  ```
  Context: All services (Kafka, consumers, API, Redis, Prometheus, Grafana) run on one t3.large.
  Decision: Accepted for portfolio — adding HA infrastructure doubles complexity and cost with no learning benefit here.
  Production path: ECS/EKS for app layer, MSK for Kafka, ElastiCache for Redis, RDS Multi-AZ already in place.
  Interview note: When asked "what's your biggest reliability risk?" — this is your answer, plus the production path.
  ```
- [ ] `RUNBOOK.md` — create: how to deploy, how to SSH in, how to restart services
- [ ] `AI_BUILD_LOG.md` — log Terraform module structure decisions
- [ ] `ARCHITECTURE.md` — fill AWS infrastructure section

## ✅ Eval: Phase 1 is Done When

- A real GitHub webhook event flows end-to-end: webhook → S3 → Kafka → Postgres → Redis → API → dashboard — verified live
- Dashboard is accessible at the CloudFront URL, not just localhost
- WebSocket live feed updates in browser when a new event is ingested
- `/health` returns 200, ALB health check is green
- All Terraform resources created with no manual console steps
- `AI_BUILD_LOG.md` has at least 8 entries
- `DECISIONS.md` has at least 6 ADRs
- `ARCHITECTURE.md` has all major sections filled

## Results to Record (Phase 1)

After Phase 1 is deployed and working, measure and log these in `PERFORMANCE.md`:

- [ ] Webhook receiver P99 response time under normal load
- [ ] End-to-end latency: webhook received → event appears in dashboard
- [ ] Kafka consumer lag at steady state
- [ ] Redis cache hit ratio on metric endpoints
- [ ] How many events/sec before consumer falls behind (rough stress test)

> **Measurement tool:** use `locust` (Python-based load testing, already in your ecosystem) for webhook stress tests and API latency measurements. Add `locust` to dev dependencies. Write a `locustfile.py` with tasks for webhook POST and API GET endpoints. This is your methodology for all `PERFORMANCE.md` numbers — not ad-hoc curl timing.

---

---

# PHASE 2 — Production Hardening (Second 50%)

> Goal: observability, CI/CD, and documentation. Project becomes genuinely shippable.

---

## Learning Checkpoints

- [ ] **Prometheus data model**: What is a counter vs a gauge vs a histogram? Why does it matter which you use?
- [ ] **Grafana vs application metrics**: Grafana visualizes Prometheus data — it doesn't collect anything itself. Can you explain the full scrape flow?
- [ ] **GitHub Actions secrets**: Where do AWS credentials live in CI/CD? Why can't they be in the repo?
- [ ] **CloudFront cache invalidation**: Why do you need to invalidate after a frontend deploy? What happens if you don't?

---

## 2.1 — Observability

### Prometheus

- [ ] Scrape: FastAPI `/metrics` (15s), Node Exporter, Kafka JMX exporter
- [ ] Retention: 15 days

### Grafana Dashboards

- [ ] Business Dashboard: PRs/day, cycle time trend, sprint status, consumer lag
- [ ] Infra Dashboard: request rate, P99 latency, error rate, Postgres connections, Redis memory
- [ ] Export both as JSON → commit to `infra/grafana/`

### Alerting

- [ ] Alert: consumer lag > 1000 for > 5 min
- [ ] Alert: error rate > 5% for > 2 min
- [ ] Notification to email or webhook

### Documentation Tasks

- [ ] `DECISIONS.md` — ADR: why Prometheus pull model over push
- [ ] `PERFORMANCE.md` — record baseline metrics from Grafana after 24h of operation
- [ ] `AI_BUILD_LOG.md` — log Grafana dashboard JSON generation process

---

## 2.2 — CI/CD

### GitHub Actions

- [ ] `ci.yml`: lint (ruff, eslint) + pytest + Docker build on every push
- [ ] `deploy-backend.yml`: SSH → EC2, pull latest, rebuild on merge to main
  - Before pulling: `git rev-parse HEAD > /tmp/last-good-sha` — save current SHA for rollback
  - After pull, before rebuild: `docker compose run --rm backend alembic upgrade head` — run migrations before starting new containers
  - After rebuild: hit `/health` — if not 200 within 30s, auto-rollback to saved SHA
  - **Why migrations before containers:** if the new code expects a column that doesn't exist yet, the app crashes on startup. Migrate first, then start.
- [ ] `RUNBOOK.md` — add migration rollback procedure:
  ```bash
  # If new code fails after migration already ran:
  alembic downgrade -1          # revert schema one step
  git checkout <prev-sha>       # revert code
  docker compose -f docker-compose.prod.yml up -d --build
  ```
- [ ] **Every Alembic migration must have a working `downgrade()` function** — test it locally before merging. A migration with an empty `downgrade()` means you can never safely roll back. Add this as a checklist item in `CONTRIBUTING.md`.
- [ ] `deploy-frontend.yml`: vite build → s3 sync → CloudFront invalidation on merge to main
- [ ] All secrets in GitHub Actions secrets

### Rollback Strategy

- [ ] `RUNBOOK.md` — add rollback procedure:
  ```bash
  # Backend rollback to previous version
  ssh ec2-user@<host>
  cd engineering-pulse
  git checkout <prev-sha>
  docker compose -f docker-compose.prod.yml up -d --build
  ```
- [ ] Frontend rollback: S3 versioning enabled on frontend bucket → `aws s3 cp s3://bucket/prev-build/ s3://bucket/ --recursive` → CloudFront invalidation
- [ ] `DECISIONS.md` — ADR: why SSH deploy vs ECR + ECS (simplicity trade-off, rollback mechanism)
- [ ] `AI_BUILD_LOG.md` — log Actions workflow debugging

### WebSocket Connection Draining on Deploy

- [ ] **Problem:** a deploy that restarts the API container drops all WS connections simultaneously — every connected user sees a frozen live feed
- [ ] **Fix:** before stopping the API container, send a WS close frame with code `4000` ("server restarting") to all connected clients
  - Add a `pre-stop.sh` script that hits an internal `/admin/drain-ws` endpoint (not exposed via ALB)
  - The endpoint iterates all active WS connections, sends close frame, waits 2 seconds for graceful disconnect
  - The frontend reconnect logic (Phase 1.4) already handles disconnects — this just makes the UX cleaner ("reconnecting..." instead of a frozen feed)
- [ ] Add to deploy script: call drain endpoint → wait 2s → `docker compose stop api` → rebuild

---

## 2.3 — Documentation Completion

### README.md (Final)

- [ ] Project description (2-3 sentences)
- [ ] Architecture diagram (Excalidraw or Mermaid)
- [ ] Tech stack table with one-line justification per tool
- [ ] Local setup: clone → `.env` → `docker compose up` → running in 5 min
- [ ] Screenshots: dashboard, Grafana, live feed
- [ ] Performance numbers from `PERFORMANCE.md`

### ARCHITECTURE.md (Final)

- [ ] Full data flow narrative
- [ ] Why each technology (1 paragraph each)
- [ ] What you'd change at 10x scale
- [ ] Known trade-offs

### TESTING.md

- [ ] What is tested: unit tests for normalization logic, integration test for webhook → Kafka
- [ ] What is not tested and why (e.g. no E2E browser tests — complexity vs value trade-off)
- [ ] How to run tests locally

### CONTRIBUTING.md

- [ ] Local setup steps
- [ ] Branch naming convention
- [ ] How to add a new event type
- [ ] How to add a new metric

### CHANGELOG.md

- [ ] Entry for Phase 1 completion with date
- [ ] Entry for Phase 2 completion with date

## ✅ Eval: Phase 2 is Done When

- Grafana shows both dashboards with real data
- A push to main triggers CI — lint + tests pass, deploy runs automatically
- README has architecture diagram, screenshots, and real performance numbers
- `DECISIONS.md` has at least 10 ADRs total
- `TESTING.md`, `CONTRIBUTING.md`, `RUNBOOK.md`, `CHANGELOG.md` all exist and are complete
- `AI_BUILD_LOG.md` has at least 15 entries total
- Someone else could clone the repo and run it locally following only the README

## Results to Record (Phase 2)

- [ ] CI pipeline run time (target: under 3 minutes)
- [ ] Grafana dashboard screenshot with 24h of real data — save to `docs/screenshots/`

---

---

# PHASE 3A — Authentication & Authorization

> Solves: no auth, no user identity
> Signal: security at the API layer, JWT lifecycle, real auth thinking
> **Do Phase 3A first, then 3B. They are separate phases with separate eval criteria.**

---

## Learning Checkpoints

- [ ] **JWT structure**: What is in a JWT payload? Why should `team_id` come from the token and not from a query parameter?
- [ ] **XSS vs CSRF**: What is each attack? How does httpOnly cookie prevent token theft via XSS?
- [ ] **Refresh token rotation**: Why rotate refresh tokens? What happens if a stolen refresh token is used after rotation?

---

## 3A.1 — Auth Layer

> **Scope warning:** Hand-rolling the full JWT lifecycle — token issuance, refresh rotation, revocation, httpOnly cookies, middleware, role checks — is a full sub-project that can easily consume 1–2 weeks on its own. It is also the part most likely to have subtle security bugs. Use a library.

### Recommended: `fastapi-users`

- [ ] Add `fastapi-users[asyncpg]` to dependencies
- [ ] Provides out of the box: registration, login, JWT + refresh token lifecycle, password hashing, httpOnly cookie transport, user model, role/permission hooks
- [ ] Customize the user model to include `org_id`, `team_id`, `role`
- [ ] Add custom middleware to extract `team_id` from the token and inject into request state — all downstream services read from request state, never from query params
- [ ] `POST /api/auth/register` — creates org + team + user in one transaction (custom logic wrapping fastapi-users registration)
- [ ] Role check dependency: `admin` manages team, `member` reads only

### Rate Limiting on Auth Endpoints

- [ ] **Rate limit `/auth/jwt/login` to 10 requests/minute per IP** — login endpoints are the #1 brute-force target
  - Use `slowapi` (already installed from Phase 1) — key function extracts client IP
  - Per-IP is correct here (unlike webhooks) because you don't know the user identity yet
  - `fastapi-users` does not include rate limiting — this is your responsibility
- [ ] Rate limit `/auth/jwt/refresh` to 30 requests/minute per IP (higher than login — legitimate apps refresh frequently)
- [ ] Rate limit `/api/auth/register` to 5 requests/minute per IP — prevents spam account creation

### Token Strategy (understand this even if the library handles it)

- **Access token:** short-lived (15 min), in memory on the frontend
- **Refresh token:** long-lived (7 days), `httpOnly` cookie — inaccessible to JavaScript, XSS-safe
- **Refresh flow:** on 401 → call `/auth/jwt/refresh` with cookie → new access token → retry
- **Why this matters for interviews:** knowing _why_ httpOnly over localStorage is the question. The library handles the _how_. Make sure you can answer: "What is an XSS attack and how does the httpOnly cookie prevent token theft?"

- [ ] `DECISIONS.md` — ADR: fastapi-users over hand-rolled auth (scope, security correctness, time trade-off)
- [ ] `DECISIONS.md` — ADR: access token in memory + httpOnly refresh cookie vs localStorage

---

## 3A.2 — WebSocket Authentication

> The WS endpoint `/ws/feed` is currently unauthenticated. Phase 3 adds JWT to all REST endpoints — the WS endpoint needs auth too or it becomes the unprotected back door.

- [ ] **Option chosen: token in first message** — client connects, immediately sends `{ "type": "auth", "token": "<access_token>" }` as the first WebSocket message
- [ ] Server validates token before subscribing to any Redis Pub/Sub channels — rejects with close code 4401 if invalid or missing
- [ ] Why not query param: tokens in URLs appear in server logs and browser history — a security leak
- [ ] `DECISIONS.md` — ADR: WS auth via first-message token vs query param vs cookie

## 3A.3 — Graceful Shutdown

> Kafka consumers need clean shutdown — commit final offsets and leave the consumer group properly. Without this, the group rebalances slowly on restart, causing processing delays.

- [ ] FastAPI lifespan already handles producer shutdown — extend it to stop consumers too
- [ ] On shutdown signal: stop polling, commit current offsets, call `consumer.stop()`
- [ ] Add `SIGTERM` handler — Docker sends SIGTERM before SIGKILL, giving you a 10-second window
- [ ] Test: `docker compose stop` → verify consumer logs show clean offset commit, not an abrupt disconnect
- [ ] `DECISIONS.md` — ADR: graceful shutdown pattern for aiokafka consumers

## ✅ Eval: Phase 3A is Done When

- Login returns JWT access token + sets httpOnly refresh cookie
- Access token expires → refresh token issues a new one automatically — user never sees a forced logout
- Refresh token expiry → user is redirected to login (graceful, not a crash)
- Attempting to access any endpoint without a valid token returns 401
- Brute-force login attempt (>10 requests/minute) returns 429
- WebSocket requires auth token in first message — unauthenticated connections are rejected with 4401
- `docker compose stop` shows clean consumer shutdown in logs

---

---

# PHASE 3B — Multi-Team & Multi-Org Support

> Solves: no multi-team or multi-org support
> Signal: tenant isolation, security at the data layer, real SaaS thinking
> **Requires Phase 3A — auth must be in place before tenant isolation makes sense**

---

## Learning Checkpoints

- [ ] **Tenant isolation**: What is the difference between application-level scoping and database-level row security (RLS)? When would you use each?
- [ ] **Foreign key cascades**: If an org is deleted, what happens to its teams and events? How do you handle this intentionally?
- [ ] **Cache namespace collisions**: What happens if two teams share the same Redis key? How does namespacing prevent this?

---

## 3B.1 — Data Model Changes

### Schema Migration

- [ ] Add `org_id` (UUID) and `team_id` (UUID) to: `pull_requests`, `commits`, `jira_tickets`, `jira_transitions`
- [ ] New tables: `orgs`, `teams`, `team_members`
- [ ] Alembic migration
- [ ] Composite indexes: `(team_id, opened_at)`, `(org_id, team_id)`

### Webhook Registration

- [ ] `POST /api/orgs/{org_id}/webhooks/register`
- [ ] Each registration stores `team_id` + `org_id` against webhook secret
- [ ] Receiver looks up `team_id` from incoming webhook secret on every event

---

## 3B.2 — Kafka Message Update

- [ ] Add `team_id` and `org_id` to every Kafka message
- [ ] Consumers extract and propagate to all writes

---

## 3B.3 — Tenant Isolation

- [ ] Every Postgres query scoped by `team_id` — no exceptions
- [ ] Redis keys namespaced: `{org_id}:{team_id}:metrics:*`
- [ ] ES documents include `team_id` as keyword field, all queries filtered by it
- [ ] Document in `ARCHITECTURE.md`: application-level scoping implemented; Postgres RLS as production upgrade path

---

## 3B.4 — Seed Script Update for Multi-Tenancy

> **This is easy to forget and will break everything if skipped.** After Phase 3B, every event requires `org_id` and `team_id`. The seed scripts from Phase 1 don't know about them.

- [ ] Update `scripts/seed.py` to:
  - Create 2 test orgs, each with 2 teams (4 teams total)
  - Associate all generated events with these orgs/teams
  - Accept `--org` and `--team` flags to seed a specific team only
- [ ] Update `scripts/seed_kafka.py` to include `org_id` and `team_id` in Kafka messages
- [ ] Verify: after seeding, querying as Team A returns only Team A's events — never Team B's

## 3B.5 — API & Frontend Updates

- [ ] All metric endpoints scoped from JWT
- [ ] New endpoints: list/create teams, list/invite members
- [ ] Frontend pages:
  - **Login page** — email + password form, calls `/auth/jwt/login`, stores access token in memory
  - **Registration page** — creates org + team + first admin user via `POST /api/auth/register`. If registration is admin-only (invite flow), skip the page and document that registration is API-only via curl/Postman.
- [ ] Team switcher in frontend header
- [ ] On team switch: invalidate all react-query cache
- [ ] Auth flow: login → JWT in memory → Bearer token on all requests

### Documentation Tasks

- [ ] `DECISIONS.md` — ADR: application-level scoping vs Postgres RLS
- [ ] `DECISIONS.md` — ADR: JWT in memory vs localStorage (security trade-off)
- [ ] `ARCHITECTURE.md` — add multi-tenancy section with isolation model diagram
- [ ] `RUNBOOK.md` — add: how to create a new org, how to rotate webhook secrets
- [ ] `CHANGELOG.md` — entry for Phase 3
- [ ] `AI_BUILD_LOG.md` — log tenant isolation approach discussions

## ✅ Eval: Phase 3B is Done When

- Two separate orgs can be created, each with their own teams
- Events from Org A are completely invisible to Org B — verified by explicit test (query as Org B, confirm Org A data never appears)
- Team switcher works in frontend — switching teams changes all dashboard data
- Seed scripts work with multi-tenancy — `seed.py` creates orgs/teams and associates events correctly
- All new ADRs logged, `ARCHITECTURE.md` multi-tenancy section complete

## Results to Record (Phase 3)

- [ ] Confirm zero cross-tenant data leakage with an explicit test — log result in `TESTING.md`
- [ ] Measure API latency impact of JWT middleware — log in `PERFORMANCE.md`
- [ ] Auth rate limiting verified: 11th login attempt within 1 minute returns 429 — log in `TESTING.md`

---

---

# PHASE 4 — Anomaly Detection & Workflow Alerting

> Solves: no anomaly detection or alerting on workflow degradation
> Signal: applied ML in a production system, real alerting thinking

---

## Learning Checkpoints

- [ ] **Rolling statistics**: What is a rolling mean and standard deviation? Why use rolling instead of all-time?
- [ ] **Why mean + 2 std deviations**: What percentage of normal values fall within this band? What does it mean when something falls outside it?
- [ ] **Prophet model assumptions**: Prophet assumes seasonality (daily/weekly patterns). Does engineering workflow data have weekly seasonality? (Yes — less activity on weekends.)
- [ ] **At what data volume does Prophet become useful**: Why wouldn't you use Prophet on 3 days of data?
- [ ] **Alert fatigue**: What happens when you alert on too many things? How do severity levels help?

---

## 4.1 — Metrics Snapshots

- [ ] Table: `metrics_snapshots` — `team_id`, `metric_name`, `value`, `recorded_at`
  - **Unique constraint:** `UNIQUE (team_id, metric_name, date_trunc('hour', recorded_at))`
  - Insert with: `INSERT INTO metrics_snapshots (...) VALUES (...) ON CONFLICT DO NOTHING`
  - This makes snapshot writes idempotent — if APScheduler fires twice in the same hour (restart race, duplicate instance), you get one row, not two. Without this, duplicate snapshots corrupt every downstream anomaly calculation.
- [ ] Table: `alert_configs` — `team_id`, `metric_name`, `std_multiplier`, `min_data_points`, `suppression_window_minutes`
- [ ] Scheduled job (APScheduler, hourly): compute and store `avg_cycle_time`, `prs_merged_count`, `tickets_opened_count`, `tickets_resolved_count` per team

> **APScheduler fragility note:** APScheduler running inside a single EC2 process is fragile — if the process restarts mid-job, you get a missed run; if two instances ever run, you get duplicate snapshot attempts. The unique constraint above prevents the duplicate data problem. The missed-run problem is acceptable for portfolio scope — document it in `ARCHITECTURE.md` as a known limitation. Production upgrade path: Celery Beat with a Redis distributed lock, or AWS EventBridge Scheduler → Lambda.

- [ ] `DECISIONS.md` — ADR: APScheduler vs Celery Beat (simplicity trade-off, known fragility, idempotency mitigation, production upgrade path)

### Snapshot Health Check

- [ ] Add `GET /api/health/snapshots` — returns the timestamp of the most recent `metrics_snapshots` row
  - If latest snapshot is >2 hours old → return `{ "status": "stale", "last_snapshot": "..." }` with HTTP 503
  - If snapshots exist and are recent → return `{ "status": "ok", "last_snapshot": "..." }` with HTTP 200
  - If no snapshots exist → return `{ "status": "no_data" }` with HTTP 200 (expected on first boot)
- [ ] This catches the silent failure mode where APScheduler stops firing but everything else looks healthy
- [ ] Add a Grafana panel for last-snapshot age — alert if >2h

---

## 4.2 — Statistical Anomaly Detection (Ship First)

- [ ] Scheduled job (hourly):
  - Fetch last 30 days of snapshots per metric per team
  - Compute rolling mean + 2 std deviations
  - Flag if current value exceeds band
- [ ] Write to `workflow_alerts`: `team_id`, `metric_name`, `expected_value`, `actual_value`, `severity`, `created_at`, `resolved_at`
- [ ] `GET /api/alerts/active`

---

## 4.3 — Robust Statistical Alerting (do this instead of Prophet)

Prophet requires ~2 years of weekly-seasonal data to give you anything the rolling-mean approach doesn't already catch. With seeded data you'd be fitting noise and calling it ML. Spend this time making the statistical layer genuinely production-quality instead.

- [ ] **Configurable thresholds per alert type:** store threshold config in Postgres (`alert_configs` table: `team_id`, `metric_name`, `std_multiplier`, `min_data_points`, `suppression_window_minutes`)
- [ ] **Suppression windows:** once an alert fires, suppress re-alerting for the same metric for N minutes (default: 60) — prevents alert storms on a sustained spike
- [ ] **Alert grouping:** if 3+ metrics spike within 5 minutes, group them into one alert with `severity: critical` instead of flooding the dashboard with individual alerts
- [ ] **Auto-resolution:** when the metric returns within the normal band, automatically mark the alert resolved with `resolved_at` timestamp
- [ ] **Minimum data points guard:** don't run anomaly detection if fewer than 7 days of snapshots exist — log a `insufficient_data` warning instead of a false alert
- [ ] Update `GET /api/alerts/active` to include grouping info and suppression status

## 4.3b — Prophet-Based Detection (Optional Extension)

> Only attempt this if the core statistical layer is solid, you have 90+ days of seeded data, and you have time. Do not include in the resume bullet unless you've actually validated it produces better results than the statistical layer.

- [ ] Add `prophet` to dependencies
- [ ] Daily job: train Prophet on last 90 days per metric per team, generate 24h forecast with 95% confidence interval
- [ ] Hourly job: compare actual vs forecast band → flag outliers
- [ ] **Validation requirement:** run both statistical and Prophet detection on the same seeded data — only keep Prophet if it catches anomalies the statistical layer misses, with a lower false positive rate. Document the comparison in `TESTING.md`.
- [ ] Store model artifacts as serialized files

---

## 4.4 — Alert Types

- [ ] Cycle Time Spike — > 2x rolling average
- [ ] PR Stall — open > 72h without activity
- [ ] Sprint Burndown Deviation — resolution rate below expected pace
- [ ] Deployment Frequency Drop — fewer deploys than expected in 7-day window
- [ ] Severity: `warning` or `critical`

---

## 4.5 — Real-Time Alert Delivery

- [ ] On anomaly write → Redis Pub/Sub `alerts:{team_id}`
- [ ] WS handler subscribes to alerts channel alongside live-feed
- [ ] Frontend: alert banner on `critical`, alert panel with full list

---

## 4.6 — Grafana Integration

- [ ] `workflow_alerts` as Grafana Postgres datasource
- [ ] Alert count panel on Business Dashboard
- [ ] Anomaly timestamp annotations on cycle time chart

### Documentation Tasks

- [ ] `DECISIONS.md` — ADR: statistical detection first, Prophet as upgrade (why not Prophet from day one)
- [ ] `DECISIONS.md` — ADR: why APScheduler over Celery Beat for scheduled jobs
- [ ] `ARCHITECTURE.md` — add anomaly detection section with model pipeline diagram
- [ ] `TESTING.md` — add: how anomaly detection is tested (seeded data with known spikes)
- [ ] `CHANGELOG.md` — entry for Phase 4
- [ ] `AI_BUILD_LOG.md` — log Prophet integration and threshold tuning decisions

## ✅ Eval: Phase 4 is Done When

- `scripts/seed.py` generates a cycle time spike on day 60 — statistical alert fires within the next hourly run
- Alert appears in dashboard in real time via WebSocket (not on page refresh)
- Sending the same alert condition twice within the suppression window → only one alert fires (suppression working)
- 3+ simultaneous metric spikes → grouped into one `critical` alert (grouping working)
- When metric returns to normal → alert auto-resolves with `resolved_at` timestamp
- All 4 alert types tested with seeded data
- `alert_configs` table allows threshold changes without code deploy — verified by changing a threshold and re-running

## Results to Record (Phase 4)

- [ ] False positive rate on 90 days of seeded data — log in `TESTING.md`
- [ ] Alert delivery latency (anomaly detected → appears in dashboard) — log in `PERFORMANCE.md`
- [ ] If Prophet implemented: comparison table vs statistical layer (false positives, missed detections) — log in `TESTING.md`

---

---

# PHASE 5 — Elasticsearch Relevance Tuning

> Solves: Elasticsearch search is basic — no relevance tuning
> Signal: you understand ES beyond just indexing documents

---

## Learning Checkpoints

- [ ] **Why not Postgres full-text search**: Postgres FTS is good. What specifically does ES do better for this use case? (Relevance scoring, cross-field queries, custom analyzers, horizontal scaling)
- [ ] **What is an analyzer**: What are the three steps ES applies to text before indexing? (Character filter → tokenizer → token filter)
- [ ] **Why camelCase tokenization matters**: A commit message says `fixAuthBug`. Without a custom analyzer, what does ES index? With one, what does it index?
- [ ] **Field boosting**: If a user searches "auth bug", should a PR titled "Fix auth bug" rank higher than a commit message mentioning "auth bug" in passing? Why?
- [ ] **Fuzziness trade-offs**: What does fuzziness cost you in terms of precision? When would you turn it off?

---

## 5.1 — Index Mapping Redesign (zero-downtime via aliases)

> "Drop and recreate" means search is down during reindex. With 90 days of seeded data this takes non-trivial time. Use ES aliases instead.

**Zero-downtime reindex pattern:**

- [ ] From the start, your app writes to and reads from an alias `engineering-events`, not the index directly
- [ ] Initial index is named `engineering-events-v1`, alias `engineering-events` points to it
- [ ] For Phase 5 redesign:
  1. Create `engineering-events-v2` with new mapping + custom analyzer
  2. Use ES `_reindex` API to copy v1 → v2 (runs in background, v1 still live)
  3. Atomically swap alias: `engineering-events` now points to v2
  4. Delete v1 once v2 is confirmed working
- [ ] App code never changes — it always reads/writes `engineering-events`

```json
// New mapping for v2
{
  "mappings": {
    "properties": {
      "title": { "type": "text", "analyzer": "engineering_analyzer" },
      "summary": { "type": "text", "analyzer": "engineering_analyzer" },
      "message": { "type": "text", "analyzer": "engineering_analyzer" },
      "author": { "type": "keyword" },
      "repo": { "type": "keyword" },
      "type": { "type": "keyword" },
      "team_id": { "type": "keyword" },
      "timestamp": { "type": "date" }
    }
  }
}
```

- [ ] `DECISIONS.md` — ADR: alias-based reindex over drop-and-recreate (zero downtime, production-correct pattern)

---

## 5.2 — Custom Analyzer

- [ ] `engineering_analyzer`:
  - Tokenizer: `standard`
  - Filters: `lowercase`, camelCase splitter, `stop`
  - Result: `pullRequest` → `pull`, `request`; `fix_auth_bug` → `fix`, `auth`, `bug`
- [ ] Test: index `title: "fixAuthBug"`, confirm match on query `"auth bug"`

---

## 5.3 — Field Boosting & Fuzzy Matching

- [ ] Query:
  ```json
  {
    "multi_match": {
      "query": "{user_query}",
      "fields": ["title^3", "summary^2", "message^1"],
      "fuzziness": "AUTO"
    }
  }
  ```

---

## 5.4 — Search API Upgrade

- [ ] `GET /api/search?q=auth&type=pull_request&author=kunal&range=30d`
- [ ] `team_id` filter always applied from JWT
- [ ] Return: `total_hits`, `results[]` with `_score` and highlight snippet
- [ ] ES highlight:
  ```json
  "highlight": { "fields": { "title": {}, "summary": {}, "message": { "fragment_size": 150 } } }
  ```
- [ ] Frontend renders highlighted snippet with matched terms bolded

---

## 5.5 — Search Quality Validation

- [ ] 10 test queries against seeded data — verify top result is correct for each
- [ ] Run same queries before and after boosting — document score improvement
- [ ] Include comparison in `ARCHITECTURE.md` as evidence of intentional tuning

### Documentation Tasks

- [ ] `DECISIONS.md` — ADR: why ES over Postgres FTS (with the specific reasons)
- [ ] `DECISIONS.md` — ADR: fuzziness AUTO vs fixed edit distance
- [ ] `ARCHITECTURE.md` — add search section with analyzer pipeline diagram
- [ ] `TESTING.md` — add: search quality test methodology and results
- [ ] `CHANGELOG.md` — entry for Phase 5
- [ ] `AI_BUILD_LOG.md` — log analyzer design decisions and test results
- [ ] `PERFORMANCE.md` — search latency before vs after tuning

## ✅ Eval: Phase 5 is Done When

- Searching `"authbug"` returns results matching `fixAuthBug` (fuzzy + camelCase)
- PR titles rank above commit messages for the same search term — verified with `_score`
- 10-query test suite passes with correct top result on all 10
- Before/after score comparison documented in `ARCHITECTURE.md`

## Results to Record (Phase 5)

- [ ] Average search latency (target: < 100ms P99) — log in `PERFORMANCE.md`
- [ ] Score improvement from boosting: average `_score` of relevant result before vs after
- [ ] 10-query test results table — log in `TESTING.md`

---

---

## Final Build Order

```
Phase 0 (foundations)
  → Phase 1 (core build — first 50%)
    → Phase 2 (hardening — second 50%)
      → Phase 3A (auth + WS auth + graceful shutdown)
        → Phase 3B (multi-tenancy + seed update)
          → Phase 4 (anomaly detection)
            → Phase 5 (search tuning)
```

Do not start Phase 3A until Phase 2 is fully deployed, documented, and passing its eval criteria.
Do not start Phase 3B until Phase 3A is passing its eval criteria — auth must exist before tenant isolation.

---

## Time Estimates (Part-Time Pace)

| Phase     | Content                                            | Comfortable pace | Terraform-new    |
| --------- | -------------------------------------------------- | ---------------- | ---------------- |
| Phase 0   | Foundations + timezone + dev workflow              | 1–2 days         | 1–2 days         |
| Phase 1   | Core pipeline + AWS                                | 25–30 days       | 30–35 days       |
| Phase 2   | Observability + CI/CD + Docs + WS drain            | 5–7 days         | 5–7 days         |
| Phase 3A  | Auth + rate limiting + WS auth + graceful shutdown | 7–10 days        | 7–10 days        |
| Phase 3B  | Multi-tenancy + seed update + isolation + frontend | 5–7 days         | 5–7 days         |
| Phase 4   | Anomaly detection + snapshot health check          | 5–7 days         | 5–7 days         |
| Phase 5   | ES tuning                                          | 3–4 days         | 3–4 days         |
| **Total** |                                                    | **~10–12 weeks** | **~12–14 weeks** |

> **Build Phase 1 locally first, deploy second.** Get the full pipeline working locally (webhook → Kafka → Postgres → Redis → API → dashboard, verified with `seed.py`) before writing a single line of Terraform. Debugging application logic locally is fast — debugging it on AWS while also learning Terraform is slow and demoralizing. The AWS deploy should be a known-working app being lifted to the cloud, not an untested app being debugged in the cloud.

---

## Resume Bullet — Base Project

> Designed and deployed a real-time dev workflow aggregator ingesting GitHub and Jira webhooks through a Kafka-backed event pipeline (3 topics, DLQ) into PostgreSQL, Redis, and Elasticsearch; served live metrics via WebSocket (Redis Pub/Sub fan-out) on a React dashboard; deployed on AWS EC2 behind ALB with CloudFront-hosted frontend, Terraform-provisioned infra, and Prometheus/Grafana observability — processing ~X events/sec with P99 latency under Y ms.

## Resume Bullet — Full Project

> Extended Engineering Pulse with multi-tenant isolation (JWT-scoped queries, team-namespaced cache keys), configurable statistical anomaly detection (rolling mean + std deviation, suppression windows, alert grouping) surfacing workflow degradation alerts in real time via WebSocket, and Elasticsearch relevance tuning (custom camelCase analyzer, field boosting, fuzzy matching) — supporting N teams across M orgs with zero cross-tenant data leakage.

_(If Prophet was implemented and validated: replace "configurable statistical anomaly detection" with "two-layer anomaly detection (statistical baseline + Prophet time-series forecasting)" — only if you have the comparison data to back it up)_
