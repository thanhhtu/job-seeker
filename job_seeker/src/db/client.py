import asyncpg
from src.core.config import settings
from src.core.logger import get_logger

logger = get_logger(__name__)

_pool: asyncpg.Pool | None = None

# libpq TCP keep-alive settings: keep idle pooled connections from being
# silently dropped by NAT/firewalls, and detect dead peers. asyncpg forwards
# these through to the underlying connection.
_KEEPALIVE_SERVER_SETTINGS = {
    "application_name": "job_seeker",
    "tcp_keepalives_idle": "60",
    "tcp_keepalives_interval": "15",
    "tcp_keepalives_count": "4",
}


def _normalize_asyncpg_dsn(dsn: str) -> str:
    # asyncpg accepts postgresql:// or postgres://, not sqlalchemy-style +asyncpg.
    if dsn.startswith("postgresql+asyncpg://"):
        return "postgresql://" + dsn[len("postgresql+asyncpg://") :]
    return dsn


async def get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        dsn = _normalize_asyncpg_dsn(settings.database_url)
        _pool = await asyncpg.create_pool(
            dsn=dsn,
            min_size=2,
            max_size=10,
            # Recycle idle connections to avoid stale/half-open sockets and to
            # let server-side resources be released after periods of inactivity.
            max_inactive_connection_lifetime=300.0,
            server_settings=_KEEPALIVE_SERVER_SETTINGS,
        )
        logger.info("PostgreSQL connection pool created")
    return _pool


async def close_pool() -> None:
    global _pool
    if _pool:
        await _pool.close()
        _pool = None
        logger.info("PostgreSQL connection pool closed")
        