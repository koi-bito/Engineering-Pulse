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
