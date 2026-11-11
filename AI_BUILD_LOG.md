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

