from pathlib import Path

from db import load_json, save_json


def build_schema(input_path: Path) -> list[dict]:
    raw_jobs = load_json(input_path)
    result = []

    for job in raw_jobs:
        skills_raw = job.get("skills") or []
        seen = set()
        skills_dedup = []
        for s in skills_raw:
            s_clean = s.strip()
            key = s_clean.lower()
            if key not in seen:
                seen.add(key)
                skills_dedup.append(s_clean)

        entry = {
            "job_id": job.get("job_id"),
            "title": job.get("title"),
            "company": job.get("company"),
            "company_id": job.get("company_id"),
            "salary": job.get("salary"),
            "location": job.get("location"),
            "work_mode": job.get("work_mode"),
            "skills": skills_dedup or None,
            "description": job.get("description"),
            "requirements": job.get("requirements"),
            "benefits": job.get("benefits"),
            "url": job.get("url"),
            "posted_at": job.get("posted_at"),
            "posted_date": job.get("posted_date"),
            "crawled_date": job.get("crawled_date"),
            "source": "itviec",
        }

        entry = {k: v for k, v in entry.items() if v is not None}
        result.append(entry)

    return result


def build_schema_file(input_path: Path) -> Path:
    output_path = input_path.with_name("itviec_jobs_schema.json")
    data = build_schema(input_path)
    save_json(output_path, data)
    print(f"  Schema saved: {output_path} ({len(data)} jobs)")
    return output_path
