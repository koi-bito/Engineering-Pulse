# Architecture

> Living document — stubbed in Phase 0 and expanded as each phase is built.

## High-Level Data Flow

```mermaid
flowchart LR
  GH[GitHub / Jira Webhooks] --> API[FastAPI Ingestion]
  API --> S3[(S3 / LocalStack Raw Archive)]
  API --> Kafka[(Kafka KRaft)]
  Kafka --> Consumers[Python Consumers]
  Consumers --> PG[(PostgreSQL)]
  Consumers --> ES[(Elasticsearch)]
  Consumers --> PubSub[(Redis Pub/Sub)]
  PG --> APIRead[FastAPI Query Layer]
  RedisCache[(Redis Cache)] --> APIRead
  ES --> APIRead
  PubSub --> WS[WebSocket /ws/feed]
  APIRead --> UI[React Dashboard]
  WS --> UI
```

## Ingestion Layer

1. **Webhook Endpoints (`backend/app/routers/webhooks.py`):**
   - `POST /webhooks/github` verifies `X-Hub-Signature-256` HMAC-SHA256 using constant-time `hmac.compare_digest` against `GITHUB_WEBHOOK_SECRET`.
   - `POST /webhooks/jira` verifies `X-Jira-Webhook-Secret` using `hmac.compare_digest` against `JIRA_WEBHOOK_SECRET`.
   - Unconfigured secrets fail closed (`500`); invalid or missing signatures return `401` using the standard `{ "error", "detail" }` error format.
2. **Per-Secret Rate Limiting (`backend/app/services/rate_limit.py`):**
   - Executed *after* signature validation using `slowapi` backed by Redis (`WEBHOOK_RATE_LIMIT`, default `100/minute`), keyed by `webhook:{source}:{sha256(secret)[:16]}`.
   - Exceeding the limit returns `429` with `Retry-After` and `{ "error": "rate_limit_exceeded", "detail", "retry_after" }`.
3. **Idempotency (`backend/app/services/idempotency.py`):**
   - Atomic `SET seen:events:{event_id} "1" EX 86400 NX` deduplicates retries and returns `202` without re-archiving or republishing.
   - If S3 or Kafka fails before ack, the key is deleted so source retries are not silently lost.
4. **S3 Raw Archive → Kafka (`backend/app/services/s3_archive.py`, `kafka_producer.py`):**
   - Raw JSON body is written asynchronously via `asyncio.to_thread(put_object)` to `raw/{source}/{YYYY}/{MM}/{DD}/{event_id}.json` in `engineering-pulse-raw-events` (LocalStack when `ENVIRONMENT=local`).
   - Canonical `WebhookEnvelope` JSON (`event_id`, `source`, `type`, `received_at`, `payload`) is then published via `AIOKafkaProducer.send_and_wait` to `github-events` or `jira-events` (`acks="all"`, `request_timeout_ms=30000`, `retry_backoff_ms=500`).

## Consumer & Storage Layer

1. **Independent Consumer Services (`backend/app/consumers/`):**
   - `github_consumer` (`github-events`), `jira_consumer` (`jira-events`) in group `engineering-pulse-consumers`, and `dlq_consumer` (`events-dlq`) in group `engineering-pulse-dlq-consumers`.
   - Each uses `enable_auto_commit=False`, `TrackingRebalanceListener` on partition revocation, and per-record commits (`{TopicPartition: offset + 1}`) only after successful persistence (or DLQ handoff).
   - Startup waits on `alembic_version` via `scripts/wait-for-migrations.sh` (`python -m app.db.wait_for_migrations`).
2. **PostgreSQL & Connection Pooling (`backend/app/db/`, `backend/alembic/`):**
   - Source-of-truth tables (`pull_requests`, `commits`, `jira_tickets`, `jira_transitions`, `failed_events`) with `TIMESTAMP WITH TIME ZONE`, indexes on query/filter columns, and idempotent upserts (`ON CONFLICT`).
   - Per-service `asyncpg.create_pool()` (`api` 5–10, `github-consumer` 2–5, `jira-consumer` 2–5, `dlq-consumer` 1–2) instrumented via `postgres_pool_active_connections{service}`.
3. **Elasticsearch (`backend/app/services/es_index.py`):**
   - Writes after PostgreSQL commit to alias `engineering-events` → `engineering-events-v1`, guarded by `ES_ENABLED`.
4. **Redis Cache & Pub/Sub (`backend/app/services/cache.py`, `pubsub.py`):**
   - Invalidates `metrics:prs_merged_today`, `metrics:avg_cycle_time_7d`, and `sprint:active:{project_key}` on writes and publishes `{ "type": "event", ... }` summaries to Redis Pub/Sub `live-feed`.

## API & WebSocket Layer

1. **Read-Through Metrics & Search REST Endpoints (`backend/app/routers/metrics.py`, `backend/app/db/repositories.py`):**
   - `GET /api/metrics/summary`, `GET /api/metrics/prs?range=7d|14d|30d|90d`, `GET /api/metrics/cycle-time?range=7d|14d|30d|90d`, `GET /api/sprint/active`, and `GET /api/search?q=...` all read Redis first via `get_or_compute_cached` (5m TTL for metrics, 1m TTL for sprint/search) before falling back to PostgreSQL or Elasticsearch (`engineering-events` alias when `ES_ENABLED=true`, PostgreSQL `ILIKE` fallback when `ES_ENABLED=false`).
   - `GET /health` returns service status, `ENVIRONMENT`, and UTC `Z` timestamp.
2. **Consistent Error Envelope (`backend/app/main.py`, `frontend/src/lib/api.ts`):**
   - Global exception handlers catch `StarletteHTTPException` (including `400`, `401`, `404`), `RequestValidationError` (`422`), `RateLimitExceeded` (`429` with `retry_after`), and unhandled `Exception` (`500`), returning `{ "error", "detail", "retry_after"? }`.
   - Frontend `axios` response interceptor extracts `detail` and `retry_after` for user-facing error states.
3. **WebSocket Live Feed (`backend/app/routers/ws.py`, `backend/app/services/pubsub.py`):**
   - `WS /ws/feed` subscribes to Redis Pub/Sub `live-feed` on connect, forwards normalized `{ "type": "event" | "alert", "source", "event_type", "timestamp", "data" }` frames, accepts client `{ "type": "auth", "token" }` messages (enforced in Phase 3A), and unsubscribes/closes Pub/Sub cleanly on disconnect.
4. **Prometheus Instrumentation (`backend/app/services/metrics.py`, `backend/app/main.py`):**
   - `prometheus-fastapi-instrumentator` exposes HTTP metrics alongside custom metrics `webhook_events_received_total`, `cache_hit_ratio`, `kafka_consumer_lag`, and `postgres_pool_active_connections` on `GET /metrics`.

## AWS Infrastructure

- **Networking (`infra/modules/vpc`):** VPC across 2 AZs with public subnets (ALB + NAT Gateway + EC2) and private subnets (RDS), isolating ports (`80/443` public on ALB → `8000` on EC2 → `5432` on RDS; SSH disabled by default in favor of AWS SSM).
- **Compute & App Stack (`infra/modules/ec2`, `docker-compose.prod.yml`):** Single `t3.large` (8GB RAM, 50GB encrypted `gp3` EBS, Docker `50m x 3` log rotation) running Kafka (KRaft, 7d/1GB retention), `kafka-init`, Redis, Prometheus (`15d`/`5GB` retention), Grafana, FastAPI, and the three Kafka consumers with `ES_ENABLED=false` during Phases 1–4.
- **Database & Secrets (`infra/modules/rds`):** Private RDS PostgreSQL 16 (`db.t3.micro`, 7-day backups, encryption at rest) using AWS-managed Secrets Manager (`manage_master_user_password = true`) plus an app secret fetched at bootstrap via a least-privilege IAM instance profile (IMDSv2, zero static AWS keys).
- **Frontend & Ingestion Edge (`infra/modules/alb`, `infra/modules/s3`, `infra/modules/cloudfront`):** ALB health-checking `GET /health` forwards API/WebSocket traffic (`120s` idle timeout) to EC2:8000; static Vite assets sit in a private versioned S3 bucket served over HTTPS via CloudFront Origin Access Control (OAC), alongside a dedicated private S3 bucket for raw webhook archives (`30d` → `STANDARD_IA`, `365d` expiration).

## Technology Choices & Rationales

- **FastAPI (Python 3.12):** Provides high-performance async concurrency required to ingest webhook bursts and stream WebSocket feeds without blocking the event loop. Pydantic v2 validates heterogeneous payloads at runtime with compiled C-speed parsing, and OpenAPI documentation is generated automatically.
- **Apache Kafka 7.6 (KRaft):** Serves as the distributed commit log buffering high-velocity external webhooks. KRaft eliminates the operational overhead of ZooKeeper, while partition-based consumer groups allow independent, crash-isolated services to consume events at their own pace without dropping messages.
- **PostgreSQL 16 (`asyncpg`):** Acts as the immutable relational source of truth for normalized developer entities (PRs, commits, tickets, transitions, and failed DLQ events). PostgreSQL offers strict ACID guarantees, JSONB semi-structured storage, and robust upsert semantics (`ON CONFLICT`) essential for idempotency.
- **Redis 7:** Delivers microsecond-latency caching for aggregated metrics to keep read endpoints responsive under heavy traffic, while Redis Pub/Sub provides a lightweight, zero-polling broadcast channel powering client WebSocket connections.
- **Elasticsearch 8.13:** Provides dedicated full-text search, fuzzy matching, and multi-field boosting (e.g., scoring PR titles higher than commit messages) across developer entities that would be expensive and slow to query via relational pattern matching.
- **Prometheus & Grafana:** Prometheus's pull model decouples metrics collection from application code, automatically flagging uncontactable instances (`up == 0`). Grafana visualizes real-time throughput, cycle time trends, and infrastructure vitals through code-provisioned dashboards.
- **React 18 + Vite + Recharts:** A modern, type-safe single-page frontend leveraging TanStack React Query for declarative server state management, Recharts for hardware-accelerated SVG visualizations, and Vite for instant build times and content-hashed asset production.
- **Terraform (AWS):** Implements infrastructure as code with modular, repeatable blueprints for VPC, RDS, EC2, S3, ALB, and CloudFront. Remote state locking in DynamoDB and S3 prevents concurrent modifications, and IAM instance profiles eliminate static AWS keys.

## Architecture at 10x Scale

If ingestion volume and query traffic increased by an order of magnitude (e.g., 50M+ events/day and thousands of concurrent users):

1. **Compute Layer (ECS / EKS Auto-Scaling):**
   - Replace the single EC2 instance with an ECS Fargate or EKS cluster spanning multiple Availability Zones.
   - Separate ingestion API pods from consumer worker pods so webhook ingress auto-scales independently based on request volume, while consumers scale based on partition lag.
2. **Managed Kafka (Amazon MSK):**
   - Migrate Kafka to Amazon MSK with Multi-AZ broker replication, automated storage auto-expansion, and dedicated JMX monitoring.
   - Increase topic partition count from 3 to 12+ partitions per topic to enable greater consumer parallelism.
3. **Database Sharding & Read Replicas:**
   - Enable RDS Multi-AZ for high availability with automated failover.
   - Deploy read replicas for heavy analytical queries (`/api/metrics/cycle-time`, `/api/metrics/prs`), routing all write traffic from consumers to the primary instance.
4. **Distributed Caching (AWS ElastiCache Redis Cluster):**
   - Upgrade single-node Redis to an ElastiCache Redis replication group with cluster mode enabled, multi-AZ failover, and in-transit encryption.
5. **Stream Processing (Apache Flink / Spark Streaming):**
   - For real-time windowed calculations (e.g., rolling 30-day percentile cycle times across millions of pull requests), introduce stream processing engines to continuously update metric materialized views instead of relying on periodic on-demand SQL aggregation.

## Known Architectural Trade-offs

- **Single EC2 Instance vs Multi-Host Orchestration:**
  - *Trade-off:* All app containers, Kafka, and Redis run on a single `t3.large`. This introduces a single point of failure (SPOF) if the EC2 host fails.
  - *Rationale:* Keeps operational costs and infrastructure complexity manageable for initial deployment. The migration path to ECS/EKS and managed services is documented and requires zero changes to application code.
- **KRaft Kafka on EC2 vs Amazon MSK:**
  - *Trade-off:* Running Kafka on EC2 requires managing broker disk space and JVM memory, whereas MSK is fully managed.
  - *Rationale:* Amazon MSK has a high minimum monthly cost (~$150/mo minimum for a 2-broker cluster). Running KRaft Kafka in a container saves significant cost while providing the exact same Kafka client protocol.
- **Eventual Consistency between PostgreSQL and Elasticsearch:**
  - *Trade-off:* Consumers commit to PostgreSQL first, then index to Elasticsearch. A brief window exists where an entity exists in PostgreSQL but has not yet appeared in Elasticsearch search results.
  - *Rationale:* PostgreSQL remains the authoritative source of truth. If Elasticsearch indexing fails, the entity is safely persisted, and the pipeline can re-index asynchronously without data loss.
- **Ephemeral Redis Cache:**
  - *Trade-off:* Redis cache keys are treated as ephemeral; a Redis restart empties the cache.
  - *Rationale:* All metrics are read-through and self-healing. When a key is missing, the API computes the value from PostgreSQL and re-populates Redis on the fly (`get_or_compute_cached`).


