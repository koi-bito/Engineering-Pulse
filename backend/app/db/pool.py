from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import asyncpg
from prometheus_client import Gauge

POSTGRES_POOL_ACTIVE_CONNECTIONS = Gauge(
    "postgres_pool_active_connections",
    "Active PostgreSQL pool connections by service",
    ["service"],
)


def normalize_asyncpg_dsn(database_url: str) -> str:
    """Convert SQLAlchemy asyncpg DSN to plain postgresql:// DSN for asyncpg."""
    return database_url.replace("postgresql+asyncpg://", "postgresql://", 1)


async def create_pg_pool(
    database_url: str,
    service: str,
    min_size: int = 2,
    max_size: int = 5,
) -> asyncpg.Pool:
    pool = await asyncpg.create_pool(
        dsn=normalize_asyncpg_dsn(database_url),
        min_size=min_size,
        max_size=max_size,
    )
    POSTGRES_POOL_ACTIVE_CONNECTIONS.labels(service=service).set(0)
    return pool


@asynccontextmanager
async def acquire_connection(
    pool: asyncpg.Pool, service: str
) -> AsyncIterator[asyncpg.Connection]:
    gauge = POSTGRES_POOL_ACTIVE_CONNECTIONS.labels(service=service)
    async with pool.acquire() as conn:
        if hasattr(pool, "get_size") and hasattr(pool, "get_idle_size"):
            gauge.set(max(pool.get_size() - pool.get_idle_size(), 0))
        else:
            gauge.inc()
        try:
            yield conn
        finally:
            if hasattr(pool, "get_size") and hasattr(pool, "get_idle_size"):
                gauge.set(max(pool.get_size() - pool.get_idle_size(), 0))
            else:
                gauge.dec()

