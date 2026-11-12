# Contributing & Local Dev Workflow

Daily development and debugging commands for the local stack.

## Infrastructure & Services

```bash
# Start local infra only (Kafka, Postgres, Redis, LocalStack, ES, Prometheus, Grafana)
docker compose up -d

# Start infra + containerized API and consumers
docker compose --profile app up -d --build

# Stop everything cleanly (preserves volumes)
docker compose --profile app stop

# Full reset (wipes all volumes and re-initializes Kafka topics/DB)
docker compose --profile app down -v && docker compose up -d
```

## Database & Logs

```bash
# Run migrations from backend/ (outside Docker)
cd backend && alembic upgrade head

# Open interactive psql shell
docker compose exec postgres psql -U postgres -d engineering_pulse

# Tail a specific consumer
docker compose --profile app logs -f github-consumer
```

## Testing & Linting

```bash
# Backend lint (from repo root)
ruff check backend scripts

# Backend unit & integration tests (from backend/)
cd backend && pytest tests -v

# Single test file, stop on first failure
cd backend && pytest tests/unit/test_normalization.py -x -v

# Frontend typecheck & production bundle
npm --prefix frontend run build
```

## Seeding Data

```bash
# Pass 1: publish directly to Kafka (consumer development, bypasses API/S3/Redis)
python scripts/seed_kafka.py --topic github-events --type pr --count 5

# Pass 2: 90-day realistic traffic through POST /webhooks/{github,jira}
python scripts/seed.py --days 90 --teams 2 --repos 3 --volume 1
```

