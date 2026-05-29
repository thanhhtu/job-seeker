from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import uuid4

from src.db.client import get_pool


@dataclass(frozen=True)
class ChatSessionRecord:
    session_id: str
    title: str | None
    created_at: datetime
    last_message_at: datetime | None
    message_count: int


@dataclass(frozen=True)
class ChatMessageRecord:
    id: int
    role: str
    content: str
    data: dict[str, Any] | None
    created_at: datetime


def _parse_message_data(raw: Any) -> dict[str, Any] | None:
    if raw is None:
        return None
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            return None
        return parsed if isinstance(parsed, dict) else None
    return raw if isinstance(raw, dict) else None


async def create_chat_session(*, user_id: str, is_guest: bool) -> str:
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


async def ensure_chat_session(
    user_id: str,
    session_id: str | None = None,
    *,
    is_guest: bool,
    adopt_client_session_id: bool = False,
) -> str:
    if not session_id:
        return await create_chat_session(user_id=user_id, is_guest=is_guest)

    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT user_id FROM chat_sessions WHERE session_id = $1 AND deleted_at IS NULL",
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
            return await create_chat_session(user_id=user_id, is_guest=is_guest)
        if row["user_id"] != user_id:
            raise ValueError("Session does not belong to user.")
    return session_id


async def add_chat_message(
    session_id: str,
    role: str,
    content: str,
    data: dict[str, Any] | None = None,
) -> None:
    pool = await get_pool()
    data_json = json.dumps(data, ensure_ascii=False) if data is not None else None
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO chat_messages(session_id, role, content, data) VALUES ($1, $2, $3, $4::jsonb)",
            session_id,
            role,
            content,
            data_json,
        )


async def get_chat_messages(session_id: str) -> list[ChatMessageRecord]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT id, role, content, data, created_at
            FROM chat_messages
            WHERE session_id = $1
            ORDER BY id ASC
            """,
            session_id,
        )
    return [
        ChatMessageRecord(
            id=row["id"],
            role=row["role"],
            content=row["content"],
            data=_parse_message_data(row["data"]),
            created_at=row["created_at"],
        )
        for row in rows
    ]


async def set_chat_session_title_if_empty(session_id: str, message: str) -> None:
    title = message.strip()[:80]
    if not title:
        return
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE chat_sessions
            SET title = $2
            WHERE session_id = $1
              AND deleted_at IS NULL
              AND (title IS NULL OR btrim(title) = '')
            """,
            session_id,
            title,
        )


async def update_chat_session_title(
    session_id: str, user_id: str, title: str
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
            WHERE session_id = $1
                AND user_id = $2
                AND is_guest = false
                AND deleted_at IS NULL
            RETURNING session_id
            """,
            session_id,
            user_id,
            trimmed[:200],
        )
    return row is not None


async def delete_chat_session(session_id: str, user_id: str) -> bool:
    """Soft-delete a session by stamping ``deleted_at``. Idempotent for already-deleted rows."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            UPDATE chat_sessions
            SET deleted_at = NOW()
            WHERE session_id = $1
                AND user_id = $2
                AND deleted_at IS NULL
            RETURNING session_id
            """,
            session_id,
            user_id,
        )
    return row is not None


async def delete_all_chat_sessions(user_id: str) -> int:
    """Soft-delete all non-guest sessions for a user. Returns number of deleted sessions."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        result = await conn.execute(
            """
            UPDATE chat_sessions
            SET deleted_at = NOW()
            WHERE user_id = $1
                AND is_guest = false
                AND deleted_at IS NULL
            """,
            user_id,
        )
    return int(result.split()[-1])


async def get_chat_session_owner(session_id: str) -> str | None:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT user_id FROM chat_sessions WHERE session_id = $1 AND deleted_at IS NULL",
            session_id,
        )
    if row is None:
        return None
    return str(row["user_id"])


async def list_chat_sessions_for_user(user_id: str) -> list[ChatSessionRecord]:
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
            WHERE s.user_id = $1
                AND s.is_guest = false
                AND s.deleted_at IS NULL
            ORDER BY s.created_at DESC
            """,
            user_id,
        )
    return [
        ChatSessionRecord(
            session_id=row["session_id"],
            title=row["title"],
            created_at=row["created_at"],
            last_message_at=row["last_message_at"],
            message_count=int(row["message_count"]),
        )
        for row in rows
    ]
