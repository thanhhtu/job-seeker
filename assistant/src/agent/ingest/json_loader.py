import json
from pathlib import Path

from agent.models import Job


async def load_jobs_from_json(path: str) -> list[Job]:
    """Load and parse a JSON file into Job dataclasses."""
    file_path = Path(path)
    if not file_path.exists():
        raise FileNotFoundError(f"JSON file not found: {path}")

    with open(file_path, encoding="utf-8") as f:
        data = json.load(f)

    return [Job.from_json(record) for record in data]


async def load_all_jobs(
    itviec_path: str = "data/itviec_jobs_schema.json",
    topcv_path: str = "data/topcv_jobs_schema.json",
) -> list[Job]:
    """Load jobs from both itviec and topcv JSON files."""
    jobs = []
    jobs.extend(await load_jobs_from_json(itviec_path))
    jobs.extend(await load_jobs_from_json(topcv_path))
    return jobs
