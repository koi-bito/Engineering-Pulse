# Changelog

All notable changes to **Engineering Pulse** are documented in this file.
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Phase 3A] - 2026-11-16

### Added
- **Authentication & User Management:**
  - Integrated `fastapi-users[sqlalchemy]` with custom user model (`id`, `email`, `hashed_password`, `org_id`, `team_id`, `role`).
  - Added Alembic migration `0002_create_users_table.py` with complete `upgrade()` and `downgrade()` definitions.
  - Implemented `/auth/jwt/login` issuing 15-minute JWT access tokens and 7-day `httpOnly`, `SameSite=Lax` refresh cookies (`ep_refresh_token`).
  - Implemented `/auth/jwt/refresh` rotating the refresh cookie and issuing fresh access tokens.
  - Implemented `/auth/jwt/logout` clearing the refresh cookie.
  - Implemented `/api/auth/register` transactionally creating organizations, teams, and initial admin users.
- **Security & Rate Limiting:**
  - IP-based `slowapi` rate limiting on auth endpoints: `/auth/jwt/login` (10/min), `/auth/jwt/refresh` (30/min), and `/api/auth/register` (5/min).
  - Role-based access control dependency (`require_role`) enforcing permission levels.
  - Protected all `/api/metrics/...`, `/api/sprint/...`, and `/api/search` endpoints requiring active authenticated users (returning 401 without valid token).
- **WebSocket First-Message Authentication:**
  - Enforced that `/ws/feed` requires an `{ "type": "auth", "token": "<access_token>" }` message within 5 seconds before subscribing to Redis channels.
  - Connections with invalid or missing tokens are closed immediately with code `4401`.
- **Graceful Consumer Teardown:**
  - Updated `BaseKafkaConsumer.stop()` to commit all pending partition offsets before stopping, preventing partition rebalances and duplicate message delivery.
- **Documentation & Decisions:**
  - Documented `ADR-025` through `ADR-028` in `DECISIONS.md`.
  - Added Phase 3A entry in `AI_BUILD_LOG.md`.

---

## [Phase 2] - 2026-11-13

### Added
- **Full Observability Stack:**
  - Automated Prometheus scrape jobs for FastAPI `/metrics`, Node Exporter host metrics, and Kafka JMX metrics (`infra/prometheus/prometheus.yml`).
  - Production alerting rules in `infra/prometheus/alert.rules.yml` for high consumer lag (`> 1000` for 5m) and elevated HTTP 5xx error rates (`> 5%` for 2m).
  - Pre-provisioned Grafana dashboards (`infra/grafana/dashboards/business-dashboard.json` and `infra/grafana/dashboards/infra-dashboard.json`) auto-loaded via Grafana provisioning YAMLs.
  - Node Exporter container added to local and production Compose stacks.
- **CI/CD Automation & Rollback Engine:**
  - GitHub Actions `ci.yml` verifying Python Ruff linting, Pytest test suite, Node TypeScript build, frontend unit tests, and Docker Compose syntax.
  - GitHub Actions `deploy-backend.yml` executing SSH deploy to EC2 with pre-pull SHA capture, pre-stop WebSocket connection draining, DB migrations before restart, and automated 30s rollback.
  - GitHub Actions `deploy-frontend.yml` building Vite bundle, syncing to S3, and invalidating `/index.html` on CloudFront.
  - WebSocket connection draining endpoint (`POST /admin/drain-ws`) and `scripts/pre-stop.sh` sending close frame code `4000` to prevent frozen UI feeds during deployments.
- **Comprehensive Documentation:**
  - `TESTING.md`: Coverage scope, trade-off justifications, and local reproduction commands.
  - `CONTRIBUTING.md`: Engineering workflow, branch naming, adding new event types, adding new metrics, and Alembic rollback requirements.
  - `DECISIONS.md`: `ADR-023` (Prometheus pull model) and `ADR-024` (Direct SSH deploy with SHA rollback).
  - Finalized `README.md`, `ARCHITECTURE.md`, and `RUNBOOK.md` rollback operations.

---

## [Phase 1] - 2026-11-12

### Added
- **Webhook Ingestion Pipeline (Phase 1.0b & 1.1):**
  - Fail-closed webhook receivers (`POST /webhooks/github`, `POST /webhooks/jira`) with HMAC-SHA256 signature verification.
  - Per-secret rate limiting with Redis-backed `slowapi` returning `429` with `Retry-After`.
  - Atomic idempotency deduplication with Redis `SET ... NX EX 86400` and failure release on pipeline error.
  - Non-blocking S3 raw body archiving and Kafka event publication (`github-events`, `jira-events`) with `acks="all"`.
- **Kafka Consumers & Storage Engine (Phase 1.2):**
  - Dedicated consumer services (`github-consumer`, `jira-consumer`, `dlq-consumer`) with manual offset management (`enable_auto_commit=False`) and per-record commits.
  - PostgreSQL 16 schema with Alembic async migrations, connection pooling, and idempotent upserts.
  - Dead-letter queue routing with failure records stored in `failed_events` table.
  - Redis cache invalidation and Pub/Sub notifications on `live-feed`.
  - Realistic 90-day historical data generation script (`scripts/seed.py`).
- **Query APIs & WebSocket Feed (Phase 1.3):**
  - Redis read-through endpoints: `/api/metrics/summary`, `/api/metrics/prs`, `/api/metrics/cycle-time`, `/api/sprint/active`, and `/api/search` with Elasticsearch fallback to Postgres.
  - WebSocket live feed (`WS /ws/feed`) streaming normalized events directly to connected clients.
  - Consistent `{ "error", "detail", "retry_after"? }` error envelope across all HTTP exceptions.
  - Prometheus custom metrics: `webhook_events_received_total`, `cache_hit_ratio`, `kafka_consumer_lag`, and `postgres_pool_active_connections`.
- **Interactive React Dashboard (Phase 1.4):**
  - Single-page application built with React 18, Vite, TypeScript, Tailwind CSS, TanStack React Query, and Recharts.
  - `OverviewPanel` with 5 KPI summary cards, throughput bar chart, cycle time line chart, and scrolling live event feed.
  - `SprintBoard` with Kanban swimlanes (`To Do`, `In Progress`, `Done`) and project filters.
  - `SearchPanel` with debounced search query execution.
  - Reconnecting `useWebSocket` hook with exponential backoff and 50-event buffer cap.
- **AWS Infrastructure & Production Compose (Phase 1.5):**
  - Modular Terraform (`infra/modules/`): VPC, RDS PostgreSQL, EC2 `t3.large`, S3 raw/frontend buckets, CloudFront (OAC), and Application Load Balancer.
  - Least-privilege IAM instance profile using IMDSv2 and AWS Secrets Manager.
  - Production Docker Compose definition (`docker-compose.prod.yml`) and operations runbook (`RUNBOOK.md`).
  - Automated Locust load test suite (`backend/locustfile.py`) and performance log (`PERFORMANCE.md`).

---

## [Phase 0] - 2026-11-11

### Added
- Repository initialization, multi-service `docker-compose.yml` (Kafka KRaft, PostgreSQL 16, Redis 7, LocalStack S3, Elasticsearch 8, Prometheus, Grafana).
- FastAPI backend scaffold with environment configuration and UTC `/health` endpoint.
- Vite + React + TypeScript frontend scaffold with Tailwind CSS.
- Initial project documentation, architecture stubs, and decisions `ADR-001` through `ADR-005`.

