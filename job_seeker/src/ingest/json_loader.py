import json
from pathlib import Path
from src.core.logger import get_logger

logger = get_logger(__name__)


def load_jobs_from_file(path: str | Path) -> list[dict]:
    """
    Load raw job data từ một JSON file.
    Hỗ trợ 2 format:
      - list: [ {...}, {...} ]
      - dict có key 'jobs': { "jobs": [...] }
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")

    with open(path, encoding="utf-8") as f:
        raw = json.load(f)

    if isinstance(raw, list):
        jobs = raw
    elif isinstance(raw, dict) and "jobs" in raw:
        jobs = raw["jobs"]
    else:
        raise ValueError(f"Unsupported JSON format in {path}")

    logger.info(f"Loaded {len(jobs)} jobs from {path.name}")
    return jobs


def load_jobs_from_dir(directory: str | Path) -> list[dict]:
    """Load tất cả .json files trong một folder."""
    directory = Path(directory)
    all_jobs = []

    for json_file in sorted(directory.glob("*.json")):
        jobs = load_jobs_from_file(json_file)
        all_jobs.extend(jobs)

    logger.info(f"Total loaded: {len(all_jobs)} jobs from {directory}")
    return all_jobs