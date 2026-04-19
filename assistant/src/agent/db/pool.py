from contextlib import asynccontextmanager
from typing import AsyncIterator

import asyncpg

from agent.config import DatabaseConfig, get_db_config


class ConnectionPool:
    __slots__ = ("_pool", "_config")

    def __init__(self, config: DatabaseConfig | None = None) -> None:
        self._pool: asyncpg.Pool | None = None
        self._config = config or get_db_config()

    async def initialize(self) -> None:
        self._pool = await asyncpg.create_pool(
            host=self._config.host,
            port=self._config.port,
            user=self._config.user,
            password=self._config.password,
            database=self._config.database,
            min_size=self._config.min_size,
            max_size=self._config.max_size,
        )

    async def close(self) -> None:
        if self._pool is not None:
            await self._pool.close()
            self._pool = None

    @asynccontextmanager
    async def acquire(self) -> AsyncIterator[asyncpg.Connection]:
        if self._pool is None:
            raise RuntimeError("Pool not initialized")
        async with self._pool.acquire() as conn:
            yield conn

    async def fetch(self, query: str, *args: object) -> list[asyncpg.Record]:
        async with self.acquire() as conn:
            return await conn.fetch(query, *args)

    async def fetchrow(self, query: str, *args: object) -> asyncpg.Record | None:
        async with self.acquire() as conn:
            return await conn.fetchrow(query, *args)

    async def fetchval(self, query: str, *args: object) -> object:
        async with self.acquire() as conn:
            return await conn.fetchval(query, *args)

    async def execute(self, query: str, *args: object) -> str:
        async with self.acquire() as conn:
            return await conn.execute(query, *args)


_global_pool: ConnectionPool | None = None


async def get_pool(reset: bool = False) -> ConnectionPool:
    global _global_pool
    if _global_pool is None or reset:
        if _global_pool is not None:
            await _global_pool.close()
        _global_pool = ConnectionPool()
        await _global_pool.initialize()
    return _global_pool


async def close_pool() -> None:
    global _global_pool
    if _global_pool is not None:
        await _global_pool.close()
        _global_pool = None
