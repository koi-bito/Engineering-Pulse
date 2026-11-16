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

---

## ADR-017 — `@tanstack/react-query` over raw `useEffect` for dashboard fetching

**Date:** 2026-11-12
**Status:** Accepted

**Context:** Dashboard panels (`summary`, `prs`, `cycle-time`, `sprint`, `search`) need loading/error states, 60-second background polling, debounced search caching, and later team-switch cache invalidation (Phase 3B).

**Decision:** Use `@tanstack/react-query` (`useQuery`) backed by typed `axios` helpers in `frontend/src/lib/api.ts` rather than manual `useEffect` + `useState` fetch logic.

**Reasons:**
- Handles request deduplication, stale-while-revalidate caching, `refetchInterval: 60_000`, and unmount race cancellation without boilerplate.
- Makes Phase 3B team switching (`queryClient.invalidateQueries()`) a one-line operation.

---

## ADR-018 — EC2 `t3.large` (8GB) over `t3.medium`, with ES excluded in Phases 1–4

**Date:** 2026-11-12
**Status:** Accepted

**Context:** Kafka (KRaft), Redis, Prometheus, Grafana, FastAPI, and three consumers co-locate on a single EC2 host; `t3.medium` (4GB RAM) risks silent OOM kills, and Elasticsearch requires ~1GB+ JVM heap.

**Decision:** Provision `t3.large` (8GB RAM, 50GB gp3 EBS with Docker log rotation) and use `docker-compose.prod.yml` with `ES_ENABLED=false` during Phases 1–4, restoring ES in Phase 5.

**Reasons:**
- Leaves comfortable memory headroom across all containers and keeps Phase 1–4 AWS costs modest (~$30/mo delta vs `t3.medium` prevents OOM instability).

---

## ADR-019 — Ephemeral Redis on EC2 accepted for portfolio scope

**Date:** 2026-11-12
**Status:** Accepted

**Context:** Running managed Multi-AZ ElastiCache adds recurring cost and complexity beyond portfolio needs, whereas the single EC2 host already runs Redis for caching, Pub/Sub, rate limiting, and webhook idempotency.

**Decision:** Run containerized Redis with persistent local volume on EC2 without cross-AZ backup/replication for portfolio scope.

**Trade-offs accepted & upgrade path:**
- On EC2 loss: cached metrics rebuild from RDS on next query, rate-limit counters reset, and 24h webhook idempotency keys clear (downstream Postgres upserts remain idempotent).
- Production upgrade path: Amazon ElastiCache for Redis with Multi-AZ replication.

---

## ADR-020 — Self-hosted Kafka (KRaft) on EC2 over Amazon MSK

**Date:** 2026-11-12
**Status:** Accepted

**Context:** Managed Amazon MSK costs significantly more per month and introduces VPC/TLS configuration overhead unnecessary for single-node portfolio traffic.

**Decision:** Run `confluentinc/cp-kafka` in KRaft mode (`1GB`/7-day log retention) inside `docker-compose.prod.yml` on EC2.

**Reasons:**
- Keeps infrastructure parity with local dev while containing cost; production upgrade path is Amazon MSK (`3` brokers across AZs).

---

## ADR-021 — EC2 IAM instance profile over static AWS access keys

**Date:** 2026-11-12
**Status:** Accepted

**Context:** The EC2 host needs access to S3 (`PutObject`/`GetObject`/`ListBucket`), Secrets Manager, and RDS without risking leaked credentials on disk or in `.env`.

**Decision:** Attach a least-privilege IAM instance profile (`engineering-pulse-ec2-profile`) using IMDSv2 (`http_tokens = "required"`) and omit static AWS keys from `docker-compose.prod.yml`.

**Reasons:**
- Credentials rotate automatically via IMDSv2 and never sit in git, Terraform variables, or `.env`.

---

## ADR-022 — Single EC2 instance as accepted SPOF for portfolio scope

**Date:** 2026-11-12
**Status:** Accepted

**Context:** Kafka, consumers, FastAPI, Redis, Prometheus, and Grafana all run on a single `t3.large` behind an ALB, while RDS and S3 sit outside the instance.

**Decision:** Accept the single EC2 instance as a conscious single point of failure for portfolio scope rather than provisioning multi-node clustering early.

**Production upgrade path:**
- Move API and consumers to ECS/EKS across 2+ AZs, Kafka to Amazon MSK, Redis to Multi-AZ ElastiCache, and enable RDS Multi-AZ.

---

## ADR-023 — Prometheus pull model over push model for observability

**Date:** 2026-11-13
**Status:** Accepted

**Context:** Observability requires gathering metrics across the FastAPI ingestion/query service, Node Exporter host vitals, and Kafka lag. We needed to choose between a pull-based collector (Prometheus scraping `/metrics`) or a push-based agent (sending metrics to an external aggregator or StatsD).

**Decision:** Use Prometheus's native pull model scraping targets every 15s.

**Reasons:**
- Decouples services from monitoring infrastructure: application containers do not need to know where Prometheus lives or handle network retries/failures if the monitoring sink is slow or down.
- Health detection is implicit: if a scrape fails or times out, Prometheus immediately knows the target is down (`up == 0`).
- Consistent scraping frequency across heterogeneous targets (FastAPI, Node Exporter, Kafka).

---

## ADR-024 — Direct SSH deploy with automatic rollback over ECR + ECS

**Date:** 2026-11-13
**Status:** Accepted

**Context:** Deploying the containerized application stack to EC2 could be done via full container image registries (AWS ECR) and orchestration (ECS Fargate/EC2) or direct git pull + Docker Compose build over SSH.

**Decision:** Implement GitHub Actions deploying directly to EC2 over SSH with pre-pull SHA capture, pre-stop WebSocket connection draining, DB migration execution, and automatic rollback on health-check failure.

**Reasons:**
- Eliminates ECR image storage/transfer costs and multi-service orchestration overhead on a single host.
- Capturing `/tmp/last-good-sha` prior to `git pull` enables instant, zero-tool rollback if post-deploy `/health` does not return `200` within 30 seconds.
- Running `alembic upgrade head` before starting new containers guarantees the database schema is compatible before incoming requests hit the updated code.

---

## ADR-025 — Use `fastapi-users` over hand-rolled authentication

**Date:** 2026-11-16
**Status:** Accepted

**Context:** The API layer needed user identity, registration, password hashing, and token lifecycle management. Hand-rolling token generation, password salt/hash rotation, and cookie transport risks subtle security vulnerabilities (timing attacks, insecure hashing, improper cookie flags) and adds significant maintenance overhead.

**Decision:** Adopt `fastapi-users[sqlalchemy]` with custom extensions for `org_id`, `team_id`, and `role`.

**Reasons:**
- Battle-tested security primitives (Argon2id/bcrypt via `pwdlib`, secure password verification, standard token models).
- Clean hooks for custom user attributes (`org_id`, `team_id`, `role`) and transactional registration wrapping org and team creation.
- Saves 1–2 weeks of low-level boilerplate, allowing focus on multi-tenancy and data pipeline guarantees.

---

## ADR-026 — Short-lived in-memory access token + httpOnly refresh cookie over localStorage

**Date:** 2026-11-16
**Status:** Accepted

**Context:** SPA applications must store credentials to authenticate requests without exposing tokens to Cross-Site Scripting (XSS) attacks. Storing long-lived JWTs in browser `localStorage` or `sessionStorage` allows any injected script to exfiltrate tokens.

**Decision:** Use short-lived (15-minute) JWT access tokens kept strictly in application memory, paired with a long-lived (7-day) `httpOnly`, `SameSite=Lax` refresh cookie (`ep_refresh_token`).

**Reasons:**
- `httpOnly` cookies are inaccessible to JavaScript via `document.cookie`, completely mitigating token theft from XSS attacks.
- Short 15-minute lifetime limits the blast radius if an access token is intercepted in flight.
- Silent refresh on 401 via `/auth/jwt/refresh` ensures seamless user experience without forced re-logins.

---

## ADR-027 — WebSocket authentication via first-message token over query parameter or cookie

**Date:** 2026-11-16
**Status:** Accepted

**Context:** Browser WebSocket API (`new WebSocket(url)`) does not support custom HTTP headers (e.g., `Authorization: Bearer <token>`). We evaluated passing tokens via URL query parameters (`/ws/feed?token=...`), relying on cookies, or exchanging an authentication frame as the first message.

**Decision:** Require client to connect and immediately send an authentication frame `{ "type": "auth", "token": "<access_token>" }` within 5 seconds before subscribing to any backend Pub/Sub channels. Unauthenticated or invalid connections are closed with code `4401`.

**Reasons:**
- Query parameters are logged in server access logs, proxy logs, and browser history, exposing bearer credentials.
- Cookie-based authentication for WebSockets introduces Cross-Site WebSocket Hijacking (CSWSH) risks.
- The first-message pattern keeps tokens out of URLs, prevents unauthorized Redis subscriptions, and provides explicit error close codes (`4401`).

---

## ADR-028 — Graceful consumer shutdown and offset commit pattern for aiokafka

**Date:** 2026-11-16
**Status:** Accepted

**Context:** Abrupt container termination (`SIGKILL` or unhandled `SIGTERM`) leaves Kafka consumer groups in an unclean state, triggering long rebalance timeouts (30+ seconds) on container restarts and risking duplicate message processing.

**Decision:** Implement signal handlers (`SIGTERM`, `SIGINT`) in `BaseKafkaConsumer` that set a stop event, commit all pending in-memory partition offsets, and cleanly call `await consumer.stop()`.

**Reasons:**
- Guarantees zero uncommitted processed messages before exit.
- Leaves the consumer group cleanly, allowing immediate partition reassignment without waiting for heartbeat timeouts.
- Fits within Docker's default 10-second graceful stop window before SIGKILL.

---

## ADR-029 — Application-Level Tenant Scoping vs PostgreSQL Row-Level Security (RLS)

**Date:** 2026-11-17
**Status:** Accepted

**Context:** Phase 3B introduces multi-team and multi-organization support across all event tables (`pull_requests`, `commits`, `jira_tickets`, `jira_transitions`). We evaluated PostgreSQL Row-Level Security (RLS) using session variables (`SET LOCAL app.current_tenant = ...`) versus application-level tenant scoping in repository queries with explicit context propagation via `request.state.team_id`.

**Decision:** Adopt application-level tenant scoping enforced by query repository helpers (`$1::uuid IS NULL OR team_id = $1::uuid`), backed by composite indexes on `(team_id, ...)` and JWT middleware propagation (`request.state.team_id` / `X-Team-ID`), instead of PostgreSQL RLS.

**Reasons:**
- Asyncpg connection pooling: RLS session settings risk state pollution across reused pooled connections if a transaction block is improperly reset.
- Predictable execution plans: Explicit SQL parameterized filters allow PostgreSQL planner to consistently leverage composite tenant indexes without dynamic session variable overhead.
- Testability and decoupling: Unit and integration tests with mocked or light connections run without requiring active database sessions or superuser privileges to configure RLS bypass policies.

---

## ADR-030 — Webhook Secret Registry and Tenant Attribution

**Date:** 2026-11-17
**Status:** Accepted

**Context:** In multi-tenant environments, incoming webhooks from GitHub and Jira must be securely attributed to the correct organization and team. GitHub and Jira webhooks do not inherently transmit custom multi-tenant headers by default.

**Decision:** Implement a webhook secret registry (`webhook_registrations` table) allowing organizations to register per-team webhook secrets (`POST /api/orgs/{org_id}/webhooks/register`), while supporting explicit client headers (`X-Org-ID`, `X-Team-ID`) and query parameters for developer workflows and internal proxies.

**Reasons:**
- Security: Secrets are uniquely mapped to teams, allowing signature verification (`X-Hub-Signature-256` or `X-Jira-Webhook-Secret`) to simultaneously authenticate the payload and identify the target tenant.
- Flexibility: Organizations can rotate individual team webhook tokens without affecting other teams or requiring global environment restarts.
- Backwards compatibility: Single-tenant fallback to `GITHUB_WEBHOOK_SECRET` and `JIRA_WEBHOOK_SECRET` remains fully functional when no team secret matches.



