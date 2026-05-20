from __future__ import annotations

from typing import Any
from uuid import uuid4

from src.db.client import get_pool


class ChatHistoryStore:
    async def create_session(self, user_id: str, *, is_guest: bool) -> str:
        session_id = str(uuid4())
        pool = await get_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO chat_sessions(session_id, user_id, is_guest)
                VALUES ($1, $2, $3)
                """,
                session_id,
                user_id,
                is_guest,
            )
        return session_id

    async def ensure_session(
        self,
        user_id: str,
        session_id: str | None = None,
        *,
        is_guest: bool,
        adopt_client_session_id: bool = False,
    ) -> str:
        if not session_id:
            return await self.create_session(user_id, is_guest=is_guest)

        pool = await get_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT user_id FROM chat_sessions WHERE session_id = $1",
                session_id,
            )
            if row is None:
                if adopt_client_session_id:
                    await conn.execute(
                        """
                        INSERT INTO chat_sessions(session_id, user_id, is_guest)
                        VALUES ($1, $2, $3)
                        """,
                        session_id,
                        user_id,
                        is_guest,
                    )
                    return session_id
                return await self.create_session(user_id, is_guest=is_guest)
            if row["user_id"] != user_id:
                raise ValueError("Session does not belong to user.")
        return session_id

    async def add_message(self, session_id: str, role: str, content: str) -> None:
        pool = await get_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO chat_messages(session_id, role, content) VALUES ($1, $2, $3)",
                session_id,
                role,
                content,
            )

    async def get_messages(self, session_id: str) -> list[dict[str, Any]]:
        pool = await get_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT id, role, content, created_at
                FROM chat_messages
                WHERE session_id = $1
                ORDER BY id ASC
                """,
                session_id,
            )
        return [dict(row) for row in rows]

    async def set_session_title_if_empty(self, session_id: str, message: str) -> None:
        title = message.strip()[:80]
        if not title:
            return
        pool = await get_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE chat_sessions
                SET title = $2
                WHERE session_id = $1 AND (title IS NULL OR btrim(title) = '')
                """,
                session_id,
                title,
            )

    async def update_session_title(
        self, session_id: str, user_id: str, title: str
    ) -> bool:
        trimmed = title.strip()
        if not trimmed:
            return False
        pool = await get_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                UPDATE chat_sessions
                SET title = $3
                WHERE session_id = $1 AND user_id = $2 AND is_guest = false
                RETURNING session_id
                """,
                session_id,
                user_id,
                trimmed[:200],
            )
        return row is not None

    async def get_session_owner(self, session_id: str) -> str | None:
        pool = await get_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT user_id FROM chat_sessions WHERE session_id = $1",
                session_id,
            )
        if row is None:
            return None
        return str(row["user_id"])

    async def list_sessions_for_user(self, user_id: str) -> list[dict[str, Any]]:
        pool = await get_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT
                    s.session_id,
                    s.title,
                    s.created_at,
                    (
                        SELECT MAX(m.created_at)
                        FROM chat_messages m
                        WHERE m.session_id = s.session_id
                    ) AS last_message_at,
                    (
                        SELECT COUNT(*)::bigint
                        FROM chat_messages m
                        WHERE m.session_id = s.session_id
                    ) AS message_count
                FROM chat_sessions s
                WHERE s.user_id = $1 AND s.is_guest = false
                ORDER BY s.created_at DESC
                """,
                user_id,
            )
        return [dict(row) for row in rows]
