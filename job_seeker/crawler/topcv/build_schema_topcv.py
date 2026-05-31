import re
from pathlib import Path

from db import load_json, save_json


def _extract_company_id(url: str) -> str | None:
    if not url:
        return None
    # url like https://www.topcv.vn/viec-lam/.../2178056.html
    m = re.search(r"/(\d+)\.html", url)
    return f"topcv-{m.group(1)}" if m else None


def _dedup_tags(tags: list[str] | None) -> list[str] | None:
    if not tags:
        return None
    seen = set()
    result = []
    for t in tags:
        key = t.strip().lower()
        if key not in seen:
            seen.add(key)
            result.append(t.strip())
    return result or None


def build_schema(input_path: Path) -> list[dict]:
    raw_jobs = load_json(input_path)
    result = []

    for job in raw_jobs:
        skills = _dedup_tags(job.get("job_expertise"))

        entry = {
            "job_id": job.get("job_id"),
            "title": job.get("title"),
            "company": job.get("company_name") or job.get("company"),
            "company_id": _extract_company_id(job.get("url")),
            "salary": job.get("salary"),
            "location": job.get("location"),
            "work_mode": job.get("work_mode"),
            "skills": skills,
            "description": job.get("description"),
            "requirements": job.get("detail_requirements"),
            "benefits": job.get("detail_benefit"),
            "url": job.get("url"),
            "deadline": job.get("deadline"),
            "experience": job.get("experience"),
            "job_level": job.get("job_level"),
            "education": job.get("education"),
            "hiring_quantity": job.get("hiring_quantity"),
            "company_size": job.get("company_size"),
            "company_industry": job.get("company_industry"),
            "working_days": job.get("working_days"),
            "detail_location": job.get("detail_location"),
            "requirements_tags": job.get("requirements_tags"),
            "benefits_tags": job.get("benefits_tags"),
            "other_benefits": job.get("other_benefits"),
            "meta_description": job.get("meta_description"),
            "crawled_date": job.get("crawled_date"),
            "source": "topcv",
        }

        entry = {k: v for k, v in entry.items() if v is not None}
        result.append(entry)

    return result


def build_schema_file(input_path: Path) -> Path:
    output_path = input_path.with_name("topcv_jobs_schema.json")
    data = build_schema(input_path)
    save_json(output_path, data)
    print(f"  Schema saved: {output_path} ({len(data)} jobs)")
    return output_path
