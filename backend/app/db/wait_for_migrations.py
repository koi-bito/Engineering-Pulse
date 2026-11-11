import asyncio
import sys

import asyncpg
import structlog

from app.config import get_settings
from app.db.pool import normalize_asyncpg_dsn

logger = structlog.get_logger(__name__)


async def wait_for_migrations(database_url: str, max_attempts: int = 30, delay_s: float = 2.0) -> None:
    dsn = normalize_asyncpg_dsn(database_url)
    for attempt in range(1, max_attempts + 1):
        try:
            conn = await asyncpg.connect(dsn=dsn)
            try:
                version = await conn.fetchval(
                    "SELECT version_num FROM alembic_version LIMIT 1"
                )
                if version:
                    logger.info("migrations_ready", version=version, attempt=attempt)
                    return
            finally:
                await conn.close()
        except (asyncpg.PostgresError, OSError) as exc:
            logger.info("waiting_for_migrations", attempt=attempt, reason=str(exc))
        await asyncio.sleep(delay_s)
    raise TimeoutError("Timed out waiting for Alembic migrations")


def main() -> None:
    settings = get_settings()
    try:
        asyncio.run(wait_for_migrations(settings.DATABASE_URL))
    except TimeoutError:
        sys.exit(1)


if __name__ == "__main__":
    main()

