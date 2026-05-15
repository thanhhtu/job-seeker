from __future__ import annotations

from dataclasses import dataclass
from uuid import uuid4

from src.db.client import get_pool


@dataclass(frozen=True)
class UserRecord:
    id: str
    email: str


async def create_user(*, email: str, password_hash: str) -> UserRecord:
    user_id = str(uuid4())
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO users(id, email, password_hash) VALUES ($1, $2, $3)",
            user_id,
            email,
            password_hash,
        )
    return UserRecord(id=user_id, email=email)


async def get_user_by_email(email: str) -> tuple[UserRecord, str] | None:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT id, email, password_hash FROM users WHERE email = $1",
            email,
        )
    if row is None:
        return None
    record = UserRecord(id=row["id"], email=row["email"])
    return record, row["password_hash"]


async def get_user_by_id(user_id: str) -> UserRecord | None:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT id, email FROM users WHERE id = $1",
            user_id,
        )
    if row is None:
        return None
    return UserRecord(id=row["id"], email=row["email"])
