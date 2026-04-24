from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Sequence
from uuid import UUID

import asyncpg

@dataclass
class Job:
    id: UUID | None = None

    job_id: str = ""
    source: str = ""
    url: str = ""
    title: str = ""

    company_name: str = ""
    company_url: str | None = None
    company_id: str | None = None
    company_size: str | None = None
    company_industry: Sequence[str] | None = None
    country: str | None = None

    salary_raw: str | None = None
    salary_min: float | None = None
    salary_max: float | None = None
    salary_currency: str | None = None
    salary_negotiable: bool = False

    location_raw: str | None = None
    locations: Sequence[str] = field(default_factory=list)

    job_domains: Sequence[str] = field(default_factory=list)
    job_level: str | None = None
    description: str = ""
    requirements: str | None = None
    skills: Sequence[str] = field(default_factory=list)
    experience_years_min: int = 0
    education: str | None = None
    benefits: str | None = None
    work_mode: str = "unknown"
    work_mode_days: str | None = None
    overtime_policy: str | None = None
    hiring_quantity: int | None = None
    deadline: date | None = None

    posted_date: datetime | None = None
    crawled_date: datetime | None = None

    created_at: datetime = field(default_factory=lambda: datetime.now())
    updated_at: datetime = field(default_factory=lambda: datetime.now())
    embedding: list[float] | None = field(default=None, repr=False)

    @classmethod
    def from_record(cls, record: asyncpg.Record) -> "Job":
        return cls(
            id=record["id"],

            job_id=record["job_id"],
            source=record["source"],
            url=record["url"],
            title=record["title"],

            company_name=record["company_name"],
            company_url=record["company_url"],
            company_id=record["company_id"],
            company_size=record["company_size"],
            company_industry=record["company_industry"],
            country=record["country"],

            salary_raw=record["salary_raw"],
            salary_min=float(record["salary_min"]) if record["salary_min"] else None,
            salary_max=float(record["salary_max"]) if record["salary_max"] else None,
            salary_currency=record["salary_currency"],
            salary_negotiable=record["salary_negotiable"],

            location_raw=record["location_raw"],
            locations=record["locations"] or [],

            job_domains=record["job_domains"] or [],
            job_level=record["job_level"],
            description=record["description"],
            requirements=record["requirements"],
            skills=record["skills"] or [],
            experience_years_min=record["experience_years_min"],
            education=record["education"],
            benefits=record["benefits"],
            work_mode=record["work_mode"],
            work_mode_days=record["work_mode_days"],
            overtime_policy=record["overtime_policy"],
            hiring_quantity=record["hiring_quantity"],
            deadline=record["deadline"],

            posted_date=record["posted_date"],
            crawled_date=record["crawled_date"],

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
            return date.fromisoformat(val[:10])

        def _datetime(val) -> datetime | None:
            if not val:
                return None
            text = str(val).strip()
            if text.endswith("Z"):
                text = text[:-1] + "+00:00"
            return datetime.fromisoformat(text)

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
            posted_date=_datetime(data.get("posted_date")),
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
            crawled_date=_datetime(data.get("crawled_date")),
        )
