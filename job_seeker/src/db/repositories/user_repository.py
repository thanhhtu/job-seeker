from __future__ import annotations

from dataclasses import dataclass
from uuid import uuid4

from src.db.client import get_pool


@dataclass(frozen=True)
class UserRecord:
    id: str
    email: str
    name: str | None = None
    phone: str | None = None


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
            "SELECT id, email, name, phone, password_hash FROM users WHERE email = $1",
            email,
        )
    if row is None:
        return None
    record = UserRecord(
        id=row["id"], email=row["email"], name=row["name"], phone=row["phone"]
    )
    return record, row["password_hash"]


async def get_user_by_id(user_id: str) -> UserRecord | None:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT id, email, name, phone FROM users WHERE id = $1",
            user_id,
        )
    if row is None:
        return None
    return UserRecord(
        id=row["id"], email=row["email"], name=row["name"], phone=row["phone"]
    )


async def update_user_profile(
    user_id: str, *, name: str | None, phone: str | None
) -> UserRecord | None:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            UPDATE users
            SET name = $2, phone = $3
            WHERE id = $1
            RETURNING id, email, name, phone
            """,
            user_id,
            name,
            phone,
        )
    if row is None:
        return None
    return UserRecord(
        id=row["id"], email=row["email"], name=row["name"], phone=row["phone"]
    )


async def get_password_hash(user_id: str) -> str | None:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT password_hash FROM users WHERE id = $1",
            user_id,
        )
    if row is None:
        return None
    return row["password_hash"]


async def update_password_hash(user_id: str, password_hash: str) -> bool:
    pool = await get_pool()
    async with pool.acquire() as conn:
        result = await conn.execute(
            "UPDATE users SET password_hash = $2 WHERE id = $1",
            user_id,
            password_hash,
        )
    return result.endswith("1")
