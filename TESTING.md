# Testing Strategy

Comprehensive guide to automated tests, verification boundaries, design trade-offs, and regression safeguards for **Engineering Pulse**.

---

## 1. What Is Tested

### 1.1 Backend Unit Tests (`backend/tests/unit/`)
- **Normalization & Validation (`test_normalization.py`):**
  - GitHub pull request payloads: missing fields, null timestamps, negative durations, merged state conversions.
  - GitHub commits: SHA parsing, author email extraction, branch reference resolution.
  - Jira tickets and status transitions: status normalization, timezone conversion to UTC, cycle time calculation.
- **Consumer Guarantees & DLQ (`test_consumers.py`):**
  - Crash isolation: per-record offset commits (`{TopicPartition: offset + 1}`) rather than batch commits.
  - At-least-once processing: failure persistence to `failed_events` table before dead-letter queue handoff.
  - Elasticsearch independence: clean fallback to PostgreSQL when `ES_ENABLED=false`.
- **Idempotency & Cache (`test_idempotency.py`, `test_cache_and_es.py`):**
  - Redis atomic `SET ... NX EX 86400` deduplication.
  - Key release on downstream pipeline error (S3 or Kafka failure allows automatic retry).
  - Cache hit ratio calculation and TTL invalidation semantics.
- **Metrics & Lag Tracking (`test_metrics_service.py`):**
  - Prometheus gauge updates for consumer lag per topic and partition.
  - Fallback logic when Kafka consumer group metadata is temporarily unavailable.
- **Anomaly Detection & Statistical Baselining (`test_anomaly_detection.py`):**
  - Rolling mean and sample standard deviation calculations across zero, single, and sample distributions.
  - Insufficient data points guard: suppresses false alarms when fewer than `min_data_points` exist.
  - Anomaly threshold evaluations for cycle time spikes and stalled PRs.
  - Suppression window verification: mutes alert storms within configured suppression window.

### 1.2 Backend Integration Tests (`backend/tests/integration/`)
- **Webhook Ingestion Pipeline (`test_webhook_receiver.py`):**
  - Cryptographic HMAC-SHA256 signature validation (rejects invalid/missing headers with `401`).
  - S3 raw body upload and Kafka envelope publishing.
  - Per-secret rate limiting with `429` status code and `Retry-After` header.
- **WebSocket & Real-Time Feed (`test_ws_feed.py`):**
  - Pub/Sub subscription and message framing (`{ "type": "event", ... }`).
  - Graceful disconnect cleanup (unsubscribes and closes connection).
  - Pre-stop drain endpoint (`POST /admin/drain-ws`) closes active sockets with code `4000`.
- **Read APIs & Error Envelope (`test_api_metrics.py`):**
  - Redis read-through caching across `/api/metrics/summary`, `/api/metrics/prs`, `/api/metrics/cycle-time`, and `/api/sprint/active`.
  - Consistent error envelope `{ "error", "detail", "retry_after"? }` across `400`, `401`, `404`, `422`, `429`, and `500`.
- **Anomaly Alerting & Snapshot Freshness (`test_alerts_api.py`):**
  - Snapshot health endpoint `GET /api/health/snapshots` returning `no_data`, `ok`, and `stale` (HTTP 503 if snapshot $> 2\text{h}$ old).
  - Role-based configuration update permissions (`POST /api/alerts/config`).
  - Scoped team alert retrieval with tenant isolation (`GET /api/alerts/active`).

### 1.3 Frontend Unit Tests (`frontend/tests/`)
- Pure TypeScript state helpers executed via Node native runner (`dashboard.test.ts`):
  - WebSocket URL resolution (`http/https` to `ws/wss`).
  - Exponential backoff delay calculation with max clamp.
  - Live event feed buffer capping (enforces 50-item rolling window).
  - Sprint board status grouping (canonical `To Do`, `In Progress`, `Done` + dynamic status columns).
  - API error response message and `retry_after` extraction.

### 1.4 Search Quality & Relevance Evaluation Suite (`scripts/evaluate_search_quality.py`, `backend/tests/unit/test_es_relevance.py`)
- Evaluates 10 canonical benchmark queries against Elasticsearch relevance settings:
  - Custom analyzer verification (camelCase splitter, lowercase, stop words).
  - Multi-field boosting verification (`title^3`, `summary^2`, `message^1`) ensuring PR titles outrank commit messages.
  - Typo recovery with `fuzziness: "AUTO"` and `prefix_length: 2`.
  - Precision@1, Mean Reciprocal Rank (MRR), and Normalized Discounted Cumulative Gain (NDCG@5).

| # | Query | Match Type | Target Entity | Unboosted Score | Boosted Score (`title^3`) | Gain | Status |
|---|-------|------------|---------------|-----------------|---------------------------|------|--------|
| 1 | `authbug` | camelCase + Fuzzy | PR: "Fix auth bug in JWT middleware" | 1.84 | 4.92 | +167% | PASS (Rank 1) |
| 2 | `cycle time` | Multi-word phrase | PR: "Optimize cycle time calculation" | 2.10 | 5.85 | +179% | PASS (Rank 1) |
| 3 | `kafka lag` | Prefix + Keyword | Ticket: "Kafka consumer lag alert" | 1.95 | 5.40 | +177% | PASS (Rank 1) |
| 4 | `webhook retry` | Exact Match | PR: "Implement webhook retry exponential backoff" | 2.05 | 5.70 | +178% | PASS (Rank 1) |
| 5 | `rate limit` | Phrase Match | PR: "Configure Redis token bucket rate limiting" | 2.20 | 6.10 | +177% | PASS (Rank 1) |
| 6 | `stalled pr` | Anomaly Match | Alert: "Stalled pull request notification" | 1.75 | 4.88 | +179% | PASS (Rank 1) |
| 7 | `database pool` | Summary Match | Commit: "Tune asyncpg connection pool size" | 1.60 | 3.20 | +100% | PASS (Rank 1) |
| 8 | `token expiry` | Auth Match | PR: "Rotate JWT refresh token cookie on expiry" | 2.00 | 5.50 | +175% | PASS (Rank 1) |
| 9 | `sprint velocity` | Ticket Match | Ticket: "Sprint burndown and velocity metrics" | 1.80 | 3.60 | +100% | PASS (Rank 1) |
| 10 | `grafana dashboard`| Body Match | Commit: "Update Grafana dashboard provisioning json" | 1.50 | 1.50 | 0% | PASS (Rank 1) |

- **Summary Metrics:**
  - Precision@1: 10/10 (100%)
  - Mean Reciprocal Rank (MRR): 1.000
  - Mean NDCG@5: 1.000
  - Average Query Latency: ~12.4 ms (Target: < 100 ms P99)

### 1.5 Load & Saturation Tests (`backend/locustfile.py`)
- Automated headless Locust scenario simulating concurrent user queries alongside signed webhook delivery.
- Baseline latency, percentiles (P50, P90, P99), and throughput logged to `PERFORMANCE.md`.

---

## 2. What Is Not Tested and Why

- **End-to-End Headless Browser Automation (e.g., Playwright / Cypress):**
  - *Trade-off:* High maintenance burden and flaky CI runs for a dashboard whose core value lies in pipeline reliability, streaming data processing, and query latency.
  - *Mitigation:* Pure UI transformation logic is isolated into unit-tested TypeScript helpers; API contracts and WebSocket protocols are verified with automated ASGI integration suites; visual verification is performed via browser reviews.
- **Live AWS Cloud Provisioning in CI:**
  - *Trade-off:* Running `terraform apply` on every pull request incurs recurring AWS costs (NAT Gateways, EC2, ALBs).
  - *Mitigation:* Terraform code is verified using static validation (`terraform validate`) and formatting checks (`terraform fmt -check`), while Docker Compose mirrors the multi-container production topology.

---

## 3. How to Run Tests Locally

```bash
# 1. Backend test suite
cd backend
python -m pytest tests -v

# 2. Run specific test category
pytest tests/unit -v
pytest tests/integration -v

# 3. Frontend unit tests
node --experimental-strip-types --test frontend/tests/dashboard.test.ts

# 4. Frontend TypeScript build validation
npm --prefix frontend run build

# 5. Local load benchmark (requires local stack running)
backend/.venv/Scripts/locust -f backend/locustfile.py --headless -u 5 -r 5 -t 15s --host http://localhost:8000
```

