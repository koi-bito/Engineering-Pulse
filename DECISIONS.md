# Architecture Decision Records (ADRs)

## ADR-001 — Monorepo structure

**Date:** 2026-11-11
**Status:** Accepted

**Context:** Engineering Pulse spans a FastAPI backend/consumers, a React frontend, and Terraform/observability config.

**Decision:** Single monorepo (`backend/`, `frontend/`, `infra/`, root `docker-compose.yml` and `.env.example`).

**Reasons:**
- Keeps shared contracts (webhook schemas, WebSocket message shapes, compose setup) versioned atomically in one commit.
- Simplifies local onboarding (`clone -> .env -> docker compose up`).

**Trade-offs accepted:**
- CI paths/workflows need filtering later so frontend-only edits don't rebuild backend images unnecessarily.

---

## ADR-002 — Kafka in KRaft mode over Zookeeper

**Date:** 2026-11-11
**Status:** Accepted

**Context:** Running Kafka on a single local machine and later on a memory-constrained EC2 `t3.large` (8GB RAM).

**Decision:** Use `confluentinc/cp-kafka:7.6.0` in KRaft mode (`broker,controller`) with `-Xmx512m -Xms512m` and `KAFKA_AUTO_CREATE_TOPICS_ENABLE="false"`.

**Reasons:**
- Eliminates Zookeeper (~500MB RAM saved and one fewer stateful container to manage).
- Explicit topic initialization via `kafka-init` ensures deterministic partition counts (`3` for event topics, `1` for DLQ).

**Trade-offs accepted:**
- Single-broker, replication factor 1 — suitable for local dev and portfolio deployment, not production HA (production would use multi-broker MSK).

---

## ADR-003 — Explicit CORS origins instead of wildcard `*`

**Date:** 2026-11-11
**Status:** Accepted

**Context:** Browser requests from `http://localhost:5173` (and later CloudFront) to FastAPI require cross-origin credentials (`httpOnly` refresh cookies in Phase 3A).

**Decision:** Configure `CORSMiddleware` with explicit origins from `CORS_ORIGINS` and `allow_credentials=True`.

**Reasons:**
- Browsers forbid `Access-Control-Allow-Origin: *` whenever credentials/cookies are sent.
- Restricting origins early prevents debugging CORS failures mid-pipeline.

---

## ADR-004 — Single `ENVIRONMENT` flag over per-service feature flags

**Date:** 2026-11-11
**Status:** Accepted

**Context:** Local development uses LocalStack and verbose logging, whereas production uses real AWS S3 and structured JSON logs.

**Decision:** Use a single `ENVIRONMENT` setting (`local` or `production`) in `config.py`, while keeping `DATABASE_URL` and `ES_ENABLED` explicit.

**Reasons:**
- Keeps parity behavior predictable with one environment switch while preventing accidental DB target mix-ups (`DATABASE_URL` stays explicit).

---

## ADR-005 — UTC timestamps everywhere, frontend converts for display

**Date:** 2026-11-11
**Status:** Accepted

**Context:** GitHub emits UTC timestamps, whereas Jira instances can emit timezone-offset timestamps.

**Decision:** Normalize and store all timestamps as UTC (`TIMESTAMP WITH TIME ZONE` in Postgres, ISO-8601 `Z` suffix in Kafka and API JSON). Convert to local time only in React UI rendering.

**Reasons:**
- Prevents timezone drift and cross-source cycle-time calculation bugs.


---

## ADR-006 — Testing scope: unit + mocked integration vs full E2E

**Date:** 2026-11-11
**Status:** Accepted

**Context:** Pipeline code spans pure Pydantic normalization, Redis idempotency, webhook authentication, Kafka/S3 wiring, and React UI.

**Decision:** Unit-test pure functions (normalization, cycle-time math, idempotency) and run ASGI integration tests against in-memory fakes for Redis/S3/Kafka; defer full E2E browser automation.

**Reasons:**
- Catches trust-boundary regressions (signature checks, validation errors, deduplication, rate limits) in <1s in CI without Docker service startup overhead.

---

## ADR-007 — Idempotency at webhook receiver (and consumer-safe storage)

**Date:** 2026-11-11
**Status:** Accepted

**Context:** GitHub and Jira retry webhook deliveries on timeouts.

**Decision:** Deduplicate at the receiver via atomic Redis `SET seen:events:{event_id} "1" EX 86400 NX` before archiving or publishing, releasing the key if S3/Kafka fails.

**Reasons:**
- Prevents duplicate deliveries from fanning out into S3 and Kafka topics while allowing failed deliveries to recover on retry.

**Trade-offs accepted:**
- Redis is ephemeral on single-EC2 deployment; 24h dedup window is bounded by Redis TTL.

---

## ADR-008 — Rate limit per webhook secret instead of client IP

**Date:** 2026-11-11
**Status:** Accepted

**Context:** Webhook providers deliver events from shared outbound IP ranges.

**Decision:** Verify webhook signatures first, then enforce `slowapi` rate limits keyed by `sha256(secret)` (falling back to client IP only when unauthenticated).

**Reasons:**
- Prevents one noisy tenant/integration from exhausting another's quota and stops unauthenticated `401` floods from draining legitimate webhook secrets.

---

## ADR-009 — `aiokafka` over `confluent-kafka-python`

**Date:** 2026-11-11
**Status:** Accepted

**Context:** FastAPI webhook ingestion and Python consumers run on asyncio event loops.

**Decision:** Use `aiokafka` (`AIOKafkaProducer` / `AIOKafkaConsumer`).

**Reasons:**
- Native asyncio integration without blocking worker threads or bridging C-library poll callbacks (`librdkafka`).

---

## ADR-010 — S3 raw payload archive before Kafka publish

**Date:** 2026-11-11
**Status:** Accepted

**Context:** Kafka can experience transient outages or consumer bugs requiring full replay.

**Decision:** Archive raw JSON payloads to S3 (`raw/{source}/{YYYY}/{MM}/{DD}/{event_id}.json`) via threadpool before `send_and_wait` to Kafka, instead of Kafka exactly-once transactions.

**Reasons:**
- Guarantees raw event durability even if Kafka publishing fails after producer retries, providing an independent replay source alongside Kafka offsets.

---

## ADR-011 — Separate Docker service per Kafka consumer

**Date:** 2026-11-11
**Status:** Accepted

**Context:** Consumers need independent restart, scaling, and crash isolation from the FastAPI ingestion/query process.

**Decision:** Run `github_consumer`, `jira_consumer`, and `dlq_consumer` as separate Docker services (`profiles: ["app"]` for local dev convenience) gated by `wait-for-migrations.sh`.

**Reasons:**
- A consumer crash or rebalance never stalls or takes down FastAPI webhook ingestion or API endpoints.

---

## ADR-012 — Per-service `asyncpg` connection pools

**Date:** 2026-11-11
**Status:** Accepted

**Context:** API and consumer containers run as separate OS processes sharing RDS `db.t3.micro` (87 max connections).

**Decision:** Each service initializes its own `asyncpg.create_pool()` (`api`: 5–10, `github-consumer`: 2–5, `jira-consumer`: 2–5, `dlq-consumer`: 1–2; worst-case 22 connections) with a `postgres_pool_active_connections{service=...}` Prometheus gauge.

**Reasons:**
- Prevents connection exhaustion under webhook bursts while staying well inside RDS limits.

---

## ADR-013 — Manual per-record offset commits after write + DLQ

**Date:** 2026-11-11
**Status:** Accepted

**Context:** `aiokafka` defaults to `enable_auto_commit=True` (at-most-once delivery risk on crash), while committing a whole `getmany` batch via bare `commit()` skips failed records.

**Decision:** Set `enable_auto_commit=False`, use idempotent Postgres upserts, commit `{TopicPartition: offset + 1}` only after Postgres/ES/PubSub succeed, and route failed records to `events-dlq` (`dlq_consumer` -> `failed_events`) before advancing the offset.

**Reasons:**
- Achieves safe at-least-once processing without letting a single malformed payload block an entire Kafka partition.

---

## ADR-014 — Elasticsearch secondary to Postgres via alias `engineering-events`

**Date:** 2026-11-11
**Status:** Accepted

**Context:** ES is excluded in production `docker-compose.prod.yml` during Phases 1–4 to conserve RAM, and Phase 5 requires zero-downtime reindexing.

**Decision:** Treat PostgreSQL as source of truth, guard ES indexing behind `ES_ENABLED`, and write to alias `engineering-events` pointing to `engineering-events-v1`.

**Reasons:**
- Consumers run cleanly in production with `ES_ENABLED=false` during Phases 1–4 and Phase 5 can swap `v1 -> v2` atomically with zero consumer code changes.

---

## ADR-015 — Producer retries + S3 archive over Kafka exactly-once

**Date:** 2026-11-11
**Status:** Accepted

**Context:** True end-to-end exactly-once semantics across webhooks, Kafka, PostgreSQL, and Elasticsearch require distributed transactions or idempotent producer + consumer coordination with higher operational complexity.

**Decision:** Configure `AIOKafkaProducer` with `acks="all"`, `request_timeout_ms=30000`, `retry_backoff_ms=500`, await `send_and_wait` sequentially per webhook, archive raw payloads in S3 before publishing, and release Redis idempotency keys on failure.

**Reasons:**
- Combines at-least-once delivery with idempotent downstream Postgres/ES writes while keeping S3 as a replayable fallback when Kafka is unavailable.

---

## ADR-016 — Redis Pub/Sub for WebSocket fan-out over direct consumer-to-WS

**Date:** 2026-11-12
**Status:** Accepted

**Context:** Kafka consumers (`github-consumer`, `jira-consumer`) run as separate Docker processes/containers from the FastAPI API process (`api`) that holds browser WebSocket connections (`/ws/feed`).

**Decision:** Consumers publish compact event envelopes to Redis Pub/Sub channel `live-feed`, and each `/ws/feed` handler subscribes to `live-feed` for the lifetime of the WebSocket connection.

**Reasons:**
- Decouples consumer processes from API process memory/lifecycle — consumers never track browser sockets, and multiple API workers can fan out the same stream.
- Avoids internal consumer-to-API RPC/HTTP push retries and head-of-line blocking inside the Kafka commit loop.

**Trade-offs accepted:**
- Redis Pub/Sub is fire-and-forget (at-most-once delivery for connected UI viewers); historical state remains durable in PostgreSQL and is fetched via REST on connect/reconnect.
