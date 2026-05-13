"""LangGraph Postgres checkpointer schema setup.

Migrations use CREATE INDEX CONCURRENTLY, which PostgreSQL forbids inside a
transaction. Pool connections default to implicit transactions, so setup()
must run on a dedicated connection with autocommit enabled.
"""

from __future__ import annotations

from langgraph.checkpoint.postgres import PostgresSaver
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
from psycopg import AsyncConnection, Connection
from psycopg.rows import dict_row


def postgres_conninfo_for_psycopg(url: str) -> str:
    if url.startswith("postgresql+asyncpg://"):
        return "postgresql://" + url[len("postgresql+asyncpg://") :]
    return url


async def ensure_langgraph_checkpoint_schema(conninfo: str) -> None:
    async with await AsyncConnection.connect(
        conninfo,
        autocommit=True,
        prepare_threshold=0,
        row_factory=dict_row,
    ) as conn:
        await AsyncPostgresSaver(conn).setup()


def ensure_langgraph_checkpoint_schema_sync(conninfo: str) -> None:
    """Sync migrations (e.g. LangGraph dev / sync PostgresSaver) outside a transaction."""
    with Connection.connect(
        conninfo,
        autocommit=True,
        prepare_threshold=0,
        row_factory=dict_row,
    ) as conn:
        PostgresSaver(conn).setup()
