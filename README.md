# Engineering Pulse

Real-time developer workflow aggregator ingesting GitHub and Jira webhook events through Kafka into PostgreSQL, Redis, and Elasticsearch, surfaced via FastAPI and a live React dashboard.

## Local Development Setup (Phase 0)

1. Copy environment variables:
   ```bash
   cp .env.example .env
   ```
2. Start local infrastructure (Kafka, Postgres, Redis, LocalStack, Elasticsearch, Prometheus, Grafana):
   ```bash
   docker compose up -d
   ```
3. Run the FastAPI backend locally:
   ```bash
   cd backend
   python -m venv .venv
   # Activate venv, then:
   pip install -e ".[dev]"
   uvicorn app.main:app --reload
   ```
   API runs at `http://localhost:8000` (health check: `GET /health`).
4. Run the React frontend locally:
   ```bash
   cd frontend
   npm install
   npm run dev
   ```
   Dashboard runs at `http://localhost:5173`.

## Project Structure

- `backend/` — FastAPI service, consumers, Pydantic models, Alembic migrations
- `frontend/` — Vite + React + TypeScript dashboard
- `infra/` — Prometheus config and (Phase 1.5+) Terraform / Grafana assets
- `docker-compose.yml` — Local infrastructure stack

