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

_To be filled in Phase 1.1._

## Consumer & Storage Layer

_To be filled in Phase 1.2._

## API & WebSocket Layer

_To be filled in Phase 1.3._

## AWS Infrastructure

_To be filled in Phase 1.5._

