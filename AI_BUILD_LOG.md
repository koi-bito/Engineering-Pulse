# AI Build Log

> Record of engineering problems, AI interactions, decisions taken, and lessons learned.

## 2026-11-11 · Phase 0 — Repository, Docker Compose, and App Scaffolds

**Problem I was solving:** Standing up a clean local foundation for Engineering Pulse (7-service Docker stack, FastAPI backend skeleton, Vite + React + TypeScript frontend shell, environment parity, CORS, and UTC defaults) before writing pipeline code.

**What I asked AI:** Scaffold Phase 0 according to `Building plan.md` without premature Phase 1+ boilerplate.

**What AI suggested:**
- Separate internal Docker Kafka listener (`kafka:29092`) from host-advertised listener (`localhost:9092`) so `kafka-init` inside Docker and `uvicorn`/consumers on the host can both talk to Kafka cleanly.
- Point Vite `envDir` to `..` so both backend and frontend read a single root `.env`.
- Use `pydantic-settings` (`BaseSettings`) for Pydantic v2 compatibility and include a `/health` endpoint returning UTC `Z` timestamps to verify CORS and backend-frontend wiring in Phase 0.

**What I did:** Applied the Phase 0 scaffold (`docker-compose.yml`, `.env.example`, `.gitignore`, `backend/`, `frontend/`, `infra/prometheus/prometheus.yml`, and Phase 0 doc stubs).

**What I learned:** In Confluent KRaft Docker setups, advertising only `localhost:9092` breaks container-to-container tools like `kafka-init` unless a separate internal listener (`kafka:29092`) is configured.

**Decisions logged in DECISIONS.md:** Yes — ADR-001 through ADR-005.


---

## 2026-11-11 · Phase 1.0b & 1.1 — Testing Foundation and Webhook Ingestion

**Problem I was solving:** Building fail-closed webhook endpoints (`POST /webhooks/github`, `POST /webhooks/jira`) with idempotency, per-secret rate limiting, non-blocking S3 raw archiving, Kafka publishing, and automated unit/integration tests.

**What I asked AI:** Implement Phase 1.0b + 1.1 while verifying installed `aiokafka` and `slowapi` signatures rather than assuming parameters.

**What AI suggested & verified:**
- Inspected `aiokafka.AIOKafkaProducer.__init__` (`0.15.0`): it does not accept `max_in_flight_requests_per_connection`; used `acks="all"`, `request_timeout_ms=30000`, `retry_backoff_ms=500`, and `await send_and_wait(...)` for broker-acknowledged ordering.
- Inspected `slowapi.Limiter` (`0.1.10`): `Limiter` calls storage synchronously (so its `storage_uri` uses `redis://`, while `redis.asyncio` handles async idempotency), and GitHub sends an HMAC of each body rather than the raw secret header. Validating the HMAC signature *before* checking `slowapi` (`auto_check=False`) lets the rate limiter key by `sha256(secret)` without letting unauthenticated `401` floods drain legitimate quota.
- Used atomic Redis `SET ... NX EX 86400` for deduplication and released the key on S3/Kafka failure so retries recover instead of disappearing.

**What I learned:** Always verify third-party constructor parameters against installed library signatures (`aiokafka`) and verify trust-boundary ordering (signature check -> per-secret rate limit -> idempotency -> S3 -> Kafka).

**Decisions logged in DECISIONS.md:** Yes — ADR-006 through ADR-010.

---

## 2026-11-11 · Phase 1.2 — Kafka Consumers, Postgres/Alembic, DLQ, Redis & Seed Script

**Problem I was solving:** Building crash-isolated Kafka consumers with at-least-once guarantees, idempotent Postgres storage, Elasticsearch secondary indexing, Redis cache/Pub/Sub fan-out, DLQ handling, and a realistic 90-day event generator (`scripts/seed.py`).

**What I asked AI:** Implement Phase 1.2 following `Building plan.md`, checking `aiokafka` assignor/commit behavior and `elasticsearch` 8.x signatures.

**What AI suggested & verified:**
- Inspected `aiokafka.AIOKafkaConsumer` (`0.15.0`): `commit()` without arguments commits all fetched offsets from `getmany()`, so committing `{TopicPartition: record.offset + 1}` per record prevents skipping failed records on crash. Also verified `RoundRobinPartitionAssignor` handles asymmetric topic subscriptions (`github-events` vs `jira-events`) in the same `engineering-pulse-consumers` group.
- Used `postgresql+asyncpg` async Alembic `env.py` + `asyncpg` polling inside `wait-for-migrations.sh` so consumers wait for migrations cleanly inside `python:3.12-slim` without needing `psql`.
- Created index `engineering-events-v1` behind alias `engineering-events` from day one so Phase 5's zero-downtime reindex needs no consumer changes.
- Designed `scripts/seed.py` (Pass 2) with weekend dips, 14-day sprint-end spikes, burst deploys, stalled PRs, and `429 Retry-After` handling so 90-day seeding works reliably against the webhook receiver.
- Containerized integration testing with `--volume medium` uncovered three real edge-case bugs that unit mocks had missed: missing `sqlalchemy[asyncio]` (`greenlet`) in non-dev deps for `ep-api` migrations, a stale `Content-Length` crash in `rate_limit_handler` on `429` responses, and future/inverted timestamps generated on `day_offset == 0` in `scripts/seed.py` — all fixed with regression assertions added.

**What I learned:** Committing per-record `TopicPartition` offsets—rather than bare `consumer.commit()`—is essential when pairing `getmany()` with manual offset management, and exercising real Uvicorn + container builds catches `ASGITransport`/dev-venv blind spots early.

**Decisions logged in DECISIONS.md:** Yes — ADR-011 through ADR-015.

---

## 2026-11-12 · Phase 1.3 — Read API, WebSocket Live Feed, Error Envelope & Prometheus Metrics

**Problem I was solving:** Building the Redis-first REST query endpoints (`/api/metrics/summary`, `/api/metrics/prs`, `/api/metrics/cycle-time`, `/api/sprint/active`, `/api/search`), consistent error responses across FastAPI and `axios`, `WS /ws/feed` via Redis Pub/Sub, Prometheus `/metrics`, and designing the client WebSocket reconnect strategy ahead of Phase 1.4.

**What I asked AI:** Implement Phase 1.3 cleanly while verifying `prometheus-fastapi-instrumentator`, `redis.asyncio.PubSub`, and `aiokafka` signatures and aligning the WebSocket message/reconnect contract for Phase 1.4 + 3A.

**What AI suggested & verified:**
- Fixed a subtle FastAPI exception-handler gap in `main.py`: registering `StarletteHTTPException` (instead of only `fastapi.HTTPException`) ensures unknown-route `404` and `405` responses use the standard `{ "error", "detail" }` JSON envelope alongside `400`, `401`, `422`, `429`, and `500`.
- Extracted a shared `get_or_compute_cached` read-through helper in `app/services/cache.py` that populates the documented cache keys (`metrics:prs_merged_today`, `metrics:avg_cycle_time_7d`, `sprint:active:*`) and updates the Prometheus `cache_hit_ratio` gauge on every lookup, while invalidating range and `"all"` sprint keys on consumer writes.
- Normalized `publish_live_event` and `WS /ws/feed` so every frame carries `{ "type", "source", "event_type", "timestamp", "data" }` and runs concurrent client-receive and `pubsub.get_message` tasks so disconnects unsubscribe immediately.
- Planned the Phase 1.4 `useWebSocket` reconnect strategy: exponential backoff on drop, pre-reconnect refresh via `/auth/jwt/refresh` (once Phase 3A auth is active) so expired 15-minute access tokens never cause infinite `4401` reconnect loops, and capping live feed state at 50 items.
- Instrumented `prometheus-fastapi-instrumentator` plus `webhook_events_received_total`, `cache_hit_ratio`, and periodic `kafka_consumer_lag` polling via `AIOKafkaAdminClient.list_consumer_group_offsets` + `AIOKafkaConsumer.end_offsets`.

**What I learned:** In FastAPI, catching `fastapi.HTTPException` misses router-level `404`s raised by Starlette; and running concurrent reader/forwarder tasks in WebSocket endpoints avoids waiting on Pub/Sub timeouts to detect browser disconnects.

**Decisions logged in DECISIONS.md:** Yes — ADR-016.

---

## 2026-11-12 · Phase 1.4 — React Dashboard, Recharts Panels, Debounced Search & Reconnecting WebSocket

**Problem I was solving:** Building the Phase 1.4 React dashboard (`OverviewPanel`, `SprintBoard`, `SearchPanel`), live `useWebSocket` hook with 50-event cap and token-refresh-ready reconnects, loading skeletons, error recovery, 60s auto-refresh, and responsive 1280px layout.

**What I asked AI:** Implement Phase 1.4 using the existing `@tanstack/react-query`, `axios`, and `recharts` dependencies and align the UI payloads with Phase 1.3's API and WebSocket protocol.

**What AI suggested & verified:**
- Implemented `useWebSocket` in `frontend/src/hooks/useWebSocket.ts` with exponential backoff (`1s` → `30s` cap), 50-item event cap, `4401` loop prevention, and an optional `refreshToken` callback before reconnecting so Phase 3A JWT expiry refreshes cleanly instead of looping.
- Built `OverviewPanel` (5 summary KPIs, 14-day Recharts `BarChart`, 30-day rolling average `LineChart`, and scrolling live feed), `SprintBoard` (canonical `To Do` / `In Progress` / `Done` + dynamic status columns and project filter), and `SearchPanel` (300ms debounced `GET /api/search?q=`), sharing `SkeletonBlock` and `ErrorBanner` in `PanelState.tsx`.
- Extracted pure TypeScript helpers (`groupTicketsByStatus`, `getWebSocketUrl`, `parseLiveFeedMessage`, `prependLiveEvent`, `nextReconnectDelayMs`) into `.ts` modules so `node --test tests/dashboard.test.ts` verifies them natively alongside `tsc -b && vite build`.

**What I learned:** Separating pure state/mapping logic from `.tsx` components lets Node's built-in test runner verify frontend logic directly without adding Jest/Vitest dependencies.

**Decisions logged in DECISIONS.md:** Yes — ADR-017.

---

## 2026-11-12 · Phase 1.5 — AWS Infrastructure (Terraform), Production Compose & Runbook

**Problem I was solving:** Provisioning reproducible AWS infrastructure (VPC, ALB, EC2 `t3.large`, private RDS PostgreSQL 16, S3 buckets, CloudFront + OAC, IAM instance profile, Secrets Manager) alongside a production Compose definition (`docker-compose.prod.yml`) and operations runbook (`RUNBOOK.md`).

**What I asked AI:** Build modular Terraform under `infra/` matching `Building plan.md` Phase 1.5, enforce least-privilege networking and IAM, avoid hardcoded secrets in state/compose, and provide a production Compose file excluding local dev services (`postgres`, `localstack`, and `elasticsearch` during Phases 1–4).

**What AI suggested & verified:**
- Split Terraform into six clean modules (`vpc`, `s3`, `rds`, `cloudfront`, `ec2`, `alb`) wired via `infra/main.tf` with S3/DynamoDB remote state in `infra/backend.tf` and zero circular dependencies (`ec2` consumes `cloudfront.domain_name` for `CORS_ORIGINS`, while `alb` attaches `ec2.instance_id`).
- Used RDS `manage_master_user_password = true` plus a dedicated `aws_secretsmanager_secret` (`engineering-pulse/app-secrets`) read at boot via the EC2 IAM instance profile (IMDSv2 enforced, SSM Session Manager enabled with SSH port 22 closed by default).
- Configured `user_data.sh.tftpl` (with LF line endings enforced in `.gitattributes`) to set Docker `json-file` log rotation (`50m x 3`), URL-encode the RDS password via `jq`, write `/opt/engineering-pulse/.env` (`0600`), and boot `docker-compose.prod.yml` once Secrets Manager values exist.
- Created `RUNBOOK.md` covering S3/DynamoDB state bootstrap, SSM EC2 access, `/index.html`-only CloudFront cache invalidation, and the RDS snapshot test-restore procedure, plus `backend/locustfile.py` for Phase 1 load measurement.

**What I learned:** Using AWS-managed RDS Secrets Manager rotation (`manage_master_user_password = true`) + URL-encoding the retrieved password in bootstrap scripts eliminates plaintext DB credentials from both Terraform state and EC2 templates while preventing special-character DSN parse errors.

**Decisions logged in DECISIONS.md:** Yes — ADR-018 through ADR-022.
