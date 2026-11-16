# Performance

> Methodology: [`backend/locustfile.py`](backend/locustfile.py) (`EngineeringPulseUser`, 5 concurrent users, 20s headless run) + FastAPI `/metrics` (`prometheus_client`). Local numbers reflect Docker Desktop on Windows and are **not** a substitute for AWS production measurements.

## Phase 1 — Local Docker Stack Baseline (2026-11-12)

Stack: local `docker-compose.yml` (`api`, `github-consumer`, `jira-consumer`, `dlq-consumer`, Kafka KRaft, PostgreSQL 16, Redis 7, LocalStack S3, Elasticsearch `8.13.4` with `ES_ENABLED=true`).

| Endpoint | Reqs | Failures | Median (ms) | Avg (ms) | Max (ms) | P99 (ms) | Throughput (req/s) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `POST /webhooks/github` | 10 | 0 | 56 | 68.16 | 117.29 | 120 | 0.53 |
| `GET /api/metrics/summary` | 32 | 0 | 3 | 4.52 | 42.18 | 42 | 1.68 |
| `GET /api/metrics/prs?range=7d` | 12 | 0 | 3 | 8.13 | 50.32 | 50 | 0.63 |
| `GET /api/metrics/cycle-time?range=30d` | 16 | 0 | 4 | 3.90 | 5.67 | 6 | 0.84 |
| `GET /api/sprint/active` | 4 | 0 | 5 | 5.10 | 7.34 | 7 | 0.21 |
| `GET /health` | 7 | 0 | 2 | 9.72 | 53.64 | 54 | 0.37 |
| **Aggregated** | **81** | **0** | **3** | **13.27** | **117.29** | **120** | **4.26** |

### Prometheus Snapshot After Run

- `kafka_consumer_lag` (`github-events` p0–p2, `jira-events` p0–p2, group `engineering-pulse-consumers`): `0.0` across all 6 partitions
- `cache_hit_ratio`: `0.631` (webhook writes invalidate cached metric keys immediately, forcing refresh on the next read)
- `postgres_pool_active_connections{service="api"}`: `1.0`

## Phase 3A — 90-Day High-Volume Dataset Load Test (2026-11-17)

Stack: local `docker-compose.yml` populated with 90-day realistic historical events (1,432 GitHub PRs/commits, 543 Jira issues/transitions; 1,975 total events). Locust executed with 20 concurrent users, ramp rate 5/s, 20s runtime with authenticated JWT token and signed webhooks.

| Endpoint | Reqs | Failures | Median (ms) | Avg (ms) | Max (ms) | P99 (ms) | Throughput (req/s) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `POST /webhooks/github` | 48 | 0 (0.0%) | 54 | 51.0 | 58 | 58 | 2.50 |
| `GET /api/metrics/summary` | 100 | 0 (0.0%) | 6 | 7.0 | 47 | 47 | 5.22 |
| `GET /api/metrics/prs?range=7d` | 41 | 0 (0.0%) | 7 | 9.0 | 41 | 41 | 2.14 |
| `GET /api/metrics/cycle-time?range=30d` | 43 | 0 (0.0%) | 7 | 9.0 | 52 | 52 | 2.24 |
| `GET /api/sprint/active` | 26 | 0 (0.0%) | 8 | 8.0 | 13 | 13 | 1.36 |
| `GET /health` | 27 | 0 (0.0%) | 3 | 4.0 | 35 | 35 | 1.41 |
| **Aggregated** | **285** | **0 (0.0%)** | **7** | **15.0** | **58** | **57** | **14.87** |

### Snapshot Metrics
- **Dataset**: 90 full days across 3 repositories (`acme/service-1..3`) and 2 Jira projects (`TEAM1`, `TEAM2`).
- **Elasticsearch Search**: 140 documents indexed and scored on test query `GET /api/search?q=auth`.
- **Consumer Lag**: `0.0` across all partitions after batch ingestion.
- **Error Rate**: `0.00%` across all endpoints under 20 concurrent simulated users.

## Phase 1 — AWS Production Eval (Pending Live Deploy)

To be measured once Terraform is applied to AWS and Phase 1 Eval runs against the deployed EC2/RDS/ALB/CloudFront stack:

- [ ] Webhook receiver P99 response time under normal load
- [ ] End-to-end latency (webhook received → event appears in dashboard)
- [ ] Kafka consumer lag at steady state (AWS EC2)
- [ ] Redis cache hit ratio on metric endpoints (AWS EC2)
- [ ] Consumer saturation threshold (`events/sec` before consumer lag grows)

