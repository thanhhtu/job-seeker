from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Sequence
from uuid import UUID

import asyncpg


@dataclass
class Job:
    job_id: str = ""
    source: str = ""
    url: str = ""
    title: str = ""
    skills: Sequence[str] = field(default_factory=list)
    job_domains: Sequence[str] = field(default_factory=list)
    description: str = ""
    requirements: str | None = None
    work_mode: str = "unknown"
    country: str | None = None
    job_level: str | None = None
    education: str | None = None
    salary_min: float | None = None
    salary_max: float | None = None
    salary_currency: str | None = None
    experience_years_min: int = 0
    deadline: date | None = None
    posted_at: datetime | None = None
    locations: Sequence[str] = field(default_factory=list)
    location_raw: str | None = None
    company_name: str = ""
    company_size: str | None = None
    company_industry: Sequence[str] | None = None
    work_mode_days: str | None = None
    overtime_policy: str | None = None
    benefits: str | None = None
    hiring_quantity: int | None = None
    salary_negotiable: bool = False
    created_at: datetime = field(default_factory=lambda: datetime.now())
    updated_at: datetime = field(default_factory=lambda: datetime.now())
    id: UUID | None = None
    embedding: list[float] | None = field(default=None, repr=False)

    @classmethod
    def from_record(cls, record: asyncpg.Record) -> "Job":
        return cls(
            id=record["id"],
            job_id=record["job_id"],
            source=record["source"],
            url=record["url"],
            title=record["title"],
            skills=record["skills"] or [],
            job_domains=record["job_domains"] or [],
            description=record["description"],
            requirements=record["requirements"],
            work_mode=record["work_mode"],
            country=record["country"],
            job_level=record["job_level"],
            education=record["education"],
            salary_min=float(record["salary_min"]) if record["salary_min"] else None,
            salary_max=float(record["salary_max"]) if record["salary_max"] else None,
            salary_currency=record["salary_currency"],
            experience_years_min=record["experience_years_min"],
            deadline=record["deadline"],
            posted_at=record["posted_at"],
            locations=record["locations"] or [],
            location_raw=record["location_raw"],
            company_name=record["company_name"],
            company_size=record["company_size"],
            company_industry=record["company_industry"],
            work_mode_days=record["work_mode_days"],
            overtime_policy=record["overtime_policy"],
            benefits=record["benefits"],
            hiring_quantity=record["hiring_quantity"],
            salary_negotiable=record["salary_negotiable"],
            created_at=record["created_at"],
            updated_at=record["updated_at"],
        )

    @classmethod
    def from_json(cls, data: dict) -> "Job":
        def _float(val) -> float | None:
            if val is None or val == 0:
                return None
            return float(val)

        def _int(val) -> int | None:
            if val is None:
                return None
            return int(val)

        def _date(val) -> date | None:
            if not val:
                return None
            return date.fromisoformat(val)

        def _datetime(val) -> datetime | None:
            if not val:
                return None
            return datetime.fromisoformat(val)

        def _str_list(val) -> list[str]:
            if not val:
                return []
            if isinstance(val, str):
                return [val]
            return list(val)

        return cls(
            job_id=str(data["job_id"]),
            source=data["source"],
            url=data["url"],
            title=data["title"],
            skills=_str_list(data.get("skills")),
            job_domains=_str_list(data.get("job_domains")),
            description=data.get("description") or "",
            requirements=data.get("requirements"),
            work_mode=data.get("work_mode") or "unknown",
            country=data.get("country"),
            job_level=data.get("job_level"),
            education=data.get("education"),
            salary_min=_float(data.get("salary_min")),
            salary_max=_float(data.get("salary_max")),
            salary_currency=data.get("salary_currency"),
            experience_years_min=_int(data.get("experience_years_min")) or 0,
            deadline=_date(data.get("deadline")),
            posted_at=_datetime(data.get("posted_at")),
            locations=_str_list(data.get("locations")),
            location_raw=data.get("location_raw"),
            company_name=data.get("company_name") or "",
            company_size=data.get("company_size"),
            company_industry=_str_list(data.get("company_industry")),
            work_mode_days=data.get("work_mode_days"),
            overtime_policy=data.get("overtime_policy"),
            benefits=data.get("benefits"),
            hiring_quantity=data.get("hiring_quantity"),
            salary_negotiable=bool(data.get("salary_negotiable")),
        )
