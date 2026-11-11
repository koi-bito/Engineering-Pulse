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

