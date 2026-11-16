# Contributing & Engineering Guide

Daily development, testing, architectural conventions, and contribution workflows for **Engineering Pulse**.

---

## 1. Local Setup in 5 Minutes

1. **Clone & Configure:**
   ```bash
   git clone <repo-url>
   cd "Engineering Pulse"
   cp .env.example .env
   ```
2. **Start Infrastructure Stack:**
   ```bash
   # Starts Kafka (KRaft), PostgreSQL 16, Redis 7, LocalStack S3, Elasticsearch 8, Prometheus, Grafana, Node Exporter
   docker compose up -d
   ```
3. **Backend Setup:**
   ```bash
   cd backend
   python -m venv .venv
   # Windows: .venv\Scripts\activate | Linux/macOS: source .venv/bin/activate
   pip install -e ".[dev]"
   alembic upgrade head
   uvicorn app.main:app --reload
   ```
4. **Frontend Setup:**
   ```bash
   cd ../frontend
   npm install
   npm run dev
   ```
5. **Verify:**
   - API & Health: `http://localhost:8000/health`
   - React Dashboard: `http://localhost:5173`
   - Grafana (provisioned dashboards): `http://localhost:3000` (admin / admin)
   - Prometheus: `http://localhost:9090`

---

## 2. Branch & Git Conventions

- **Branch Naming:**
  - `feat/<short-description>` (e.g., `feat/add-pagerduty-events`)
  - `fix/<short-description>` (e.g., `fix/consumer-rebalance-deadlock`)
  - `chore/<short-description>` (e.g., `chore/bump-aiokafka`)
- **Commit Messages:**
  - Follow Conventional Commits format: `<type>(<scope>): <summary>` (e.g., `feat(webhooks): add gitlab webhook handler`).
- **Never Commit Secrets:**
  - The repo contains automated secret scans. Ensure `.env` is never staged.

---

## 3. How to Add a New Event Type

Adding a new webhook event source (e.g., GitLab, Linear, PagerDuty):

1. **Webhook Receiver (`backend/app/routers/webhooks.py`):**
   - Create route `POST /webhooks/<source>`.
   - Verify cryptographic signature first (constant-time compare against configured secret).
   - Enforce rate limit via `webhook_limiter` keyed by secret hash.
   - Atomic idempotency check: `SET seen:events:{event_id} "1" EX 86400 NX`.
   - Archive raw payload to S3 (`S3ArchiveService.archive_raw_event`).
   - Publish canonical `WebhookEnvelope` to Kafka topic `<source>-events`.
2. **Kafka Producer & Topics (`docker-compose.yml`, `infra/`):**
   - Add topic definition with partitions and replication factor to `kafka-init`.
3. **Consumer Normalization (`backend/app/consumers/`):**
   - Create consumer service inheriting `BaseConsumer` with `enable_auto_commit=False`.
   - Normalize payload into validated domain models (`app/models/`).
   - Persist to PostgreSQL with idempotent upsert (`ON CONFLICT`).
   - If `ES_ENABLED=true`, index document in `engineering-events` alias.
   - Invalidate affected Redis cache keys and publish update to `live-feed` Pub/Sub.
4. **Tests:**
   - Add unit tests for normalization under `backend/tests/unit/`.
   - Add integration tests for the webhook route under `backend/tests/integration/`.

---

## 4. How to Add a New Metric

1. **Prometheus Gauge/Counter (`backend/app/services/metrics.py`):**
   - Define metric instance with appropriate labels (use counters for monotonic counts, gauges for levels/ratios, histograms for latency).
2. **API Endpoint / Cache (`backend/app/routers/metrics.py`, `backend/app/services/cache.py`):**
   - Wrap DB query in `get_or_compute_cached` with documented TTL (default 5m).
   - Invalidate key on relevant consumer writes.
3. **Grafana Dashboard (`infra/grafana/dashboards/`):**
   - Add target panel to either `business-dashboard.json` or `infra-dashboard.json`.
4. **Locust Scenario (`backend/locustfile.py`):**
   - Add task to `EngineeringPulseUser` to measure throughput and P99 latency impact under load.

---

## 5. Database Migration Checklist

- [ ] Every migration in `backend/alembic/versions/` **MUST** implement a working, non-empty `downgrade()` function.
- [ ] Test the rollback locally before submitting PR:
  ```bash
  alembic upgrade head
  alembic downgrade -1
  alembic upgrade head
  ```
- [ ] Schema changes must be backward-compatible (expand before contract).
- [ ] Add indexes for columns used in `WHERE`, `JOIN`, or `ORDER BY` clauses.

---

## 6. Daily Testing & Quality Commands

```bash
# Lint code and scripts
ruff check backend scripts

# Run full backend test suite
cd backend && pytest tests -v

# Frontend typecheck & test suite
npm --prefix frontend run build
node --experimental-strip-types --test frontend/tests/dashboard.test.ts

# Load test against local stack
backend/.venv/Scripts/locust -f backend/locustfile.py --headless -u 5 -r 5 -t 15s --host http://localhost:8000
```
