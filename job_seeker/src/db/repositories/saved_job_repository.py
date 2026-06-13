from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Sequence
from uuid import UUID

from src.db.client import get_pool
from src.retrieval._filters import location_label

VALID_STATUSES: frozenset[str] = frozenset(
    {"saved", "applied", "interviewing", "offer", "rejected"}
)


@dataclass(frozen=True)
class SavedJobRecord:
    """A saved job joined with the columns needed to render a job card."""

    job_id: UUID
    status: str
    note: str | None
    applied_at: datetime | None
    created_at: datetime
    updated_at: datetime

    # job card fields
    title: str
    company_name: str
    url: str
    locations: list[str]
    salary_min: float | None
    salary_max: float | None
    salary_currency: str | None
    salary_negotiable: bool
    work_mode: str | None
    job_level: str | None
    skills: list[str]
    experience_years_min: int
    posted_date: datetime | None


_CARD_COLUMNS = """
    j.title,
    j.company_name,
    j.url,
    j.locations,
    j.salary_min,
    j.salary_max,
    j.salary_currency,
    j.salary_negotiable,
    j.work_mode,
    j.job_level,
    j.skills,
    j.experience_years_min,
    j.posted_date
"""


def _record_from_row(row) -> SavedJobRecord:
    return SavedJobRecord(
        job_id=row["job_id"],
        status=row["status"],
        note=row["note"],
        applied_at=row["applied_at"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        title=row["title"],
        company_name=row["company_name"],
        url=row["url"] or "",
        locations=[location_label(loc) for loc in (row["locations"] or [])],
        salary_min=float(row["salary_min"]) if row["salary_min"] else None,
        salary_max=float(row["salary_max"]) if row["salary_max"] else None,
        salary_currency=row["salary_currency"],
        salary_negotiable=bool(row["salary_negotiable"]),
        work_mode=row["work_mode"] if row["work_mode"] != "unknown" else None,
        job_level=row["job_level"],
        skills=row["skills"] or [],
        experience_years_min=row["experience_years_min"] or 0,
        posted_date=row["posted_date"],
    )


async def job_exists(job_id: UUID) -> bool:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT 1 FROM jobs WHERE id = $1", job_id)
    return row is not None


async def list_saved_jobs(
    user_id: str, *, status: str | None = None
) -> list[SavedJobRecord]:
    conditions = ["sj.user_id = $1"]
    params: list = [user_id]
    if status:
        conditions.append("sj.status = $2")
        params.append(status)

    query = f"""
        SELECT
            sj.job_id,
            sj.status,
            sj.note,
            sj.applied_at,
            sj.created_at,
            sj.updated_at,
            {_CARD_COLUMNS}
        FROM saved_jobs sj
        JOIN jobs j ON j.id = sj.job_id
        WHERE {" AND ".join(conditions)}
        ORDER BY sj.updated_at DESC
    """

    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(query, *params)
    return [_record_from_row(row) for row in rows]


async def list_saved_job_ids(user_id: str) -> list[UUID]:
    """IDs only — lets the frontend mark which cards are already saved."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT job_id FROM saved_jobs WHERE user_id = $1",
            user_id,
        )
    return [row["job_id"] for row in rows]


async def get_saved_job(user_id: str, job_id: UUID) -> SavedJobRecord | None:
    query = f"""
        SELECT
            sj.job_id,
            sj.status,
            sj.note,
            sj.applied_at,
            sj.created_at,
            sj.updated_at,
            {_CARD_COLUMNS}
        FROM saved_jobs sj
        JOIN jobs j ON j.id = sj.job_id
        WHERE sj.user_id = $1 AND sj.job_id = $2
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(query, user_id, job_id)
    return _record_from_row(row) if row is not None else None


async def upsert_saved_job(
    user_id: str,
    job_id: UUID,
    *,
    status: str = "saved",
    note: str | None = None,
) -> None:
    """Save a job (or update status/note if already saved).

    ``applied_at`` is stamped the first time the status reaches a non-``saved`` value.
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO saved_jobs (user_id, job_id, status, note, applied_at)
            VALUES (
                $1, $2, $3, $4,
                CASE WHEN $3 <> 'saved' THEN now() ELSE NULL END
            )
            ON CONFLICT (user_id, job_id) DO UPDATE SET
                status = EXCLUDED.status,
                note = EXCLUDED.note,
                applied_at = CASE
                    WHEN EXCLUDED.status <> 'saved' AND saved_jobs.applied_at IS NULL
                        THEN now()
                    ELSE saved_jobs.applied_at
                END,
                updated_at = now()
            """,
            user_id,
            job_id,
            status,
            note,
        )


async def update_saved_job(
    user_id: str,
    job_id: UUID,
    *,
    status: str | None = None,
    note: str | None = None,
) -> bool:
    """Patch status and/or note of an existing saved job. Returns False if not found."""
    sets: list[str] = ["updated_at = now()"]
    params: list = [user_id, job_id]
    idx = 3

    if status is not None:
        sets.append(f"status = ${idx}")
        params.append(status)
        idx += 1
        sets.append(
            f"applied_at = CASE WHEN ${idx - 1} <> 'saved' AND applied_at IS NULL "
            "THEN now() ELSE applied_at END"
        )
    if note is not None:
        sets.append(f"note = ${idx}")
        params.append(note)
        idx += 1

    query = f"""
        UPDATE saved_jobs
        SET {", ".join(sets)}
        WHERE user_id = $1 AND job_id = $2
        RETURNING id
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(query, *params)
    return row is not None


async def delete_saved_job(user_id: str, job_id: UUID) -> bool:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "DELETE FROM saved_jobs WHERE user_id = $1 AND job_id = $2 RETURNING id",
            user_id,
            job_id,
        )
    return row is not None
