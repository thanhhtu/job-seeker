from __future__ import annotations

from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, Field

SavedJobStatus = Literal["saved", "applied", "interviewing", "offer", "rejected"]


class SavedJobItem(BaseModel):
    """A saved job plus the fields needed to render its card on the Settings page."""

    job_id: UUID
    status: SavedJobStatus
    note: str | None = None
    applied_at: datetime | None = None
    created_at: datetime
    updated_at: datetime

    title: str
    company_name: str
    url: str = ""
    locations: list[str] = []
    salary_min: float | None = None
    salary_max: float | None = None
    salary_currency: str | None = None
    salary_negotiable: bool = False
    work_mode: str | None = None
    job_level: str | None = None
    skills: list[str] = []
    experience_years_min: int = 0
    posted_date: datetime | None = None


class SaveJobRequest(BaseModel):
    job_id: UUID
    status: SavedJobStatus = "saved"
    note: str | None = Field(default=None, max_length=2000)


class UpdateSavedJobRequest(BaseModel):
    status: SavedJobStatus | None = None
    note: str | None = Field(default=None, max_length=2000)
