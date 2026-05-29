import asyncio
from pathlib import Path

from src.ingest.json_loader import load_jobs_from_dir, load_jobs_from_file
from src.ingest.embed import embed_jobs
from src.db.repositories.job_repository import upsert_jobs
from src.db.client import close_pool
from src.core.logger import get_logger

logger = get_logger(__name__)

DATA_DIRS = [
    Path("crawler/data_job/itviec_jobs_schema.json"),
    Path("crawler/data_job/topcv_jobs_schema.json"),
]

async def run_pipeline(data_dirs: list[Path] = DATA_DIRS) -> None:
    try:
        all_raw: list[dict] = []
        for path in data_dirs:
            if not path.exists():
                logger.warning(f"Path not found, skipping: {path}")
                continue

            if path.is_dir():
                jobs = load_jobs_from_dir(path)
                all_raw.extend(jobs)
            elif path.is_file():
                jobs = load_jobs_from_file(path)
                all_raw.extend(jobs)
            else:
                logger.warning(f"Unsupported path type, skipping: {path}")
                continue

        if not all_raw:
            logger.warning("No jobs found. Pipeline exited.")
            return

        logger.info(f"Total jobs to ingest: {len(all_raw)}")

        # embed_jobs: list[dict] -> list[Job]  (parse + embed bên trong)
        jobs_with_embedding = await embed_jobs(all_raw)

        count = await upsert_jobs(jobs_with_embedding)
        logger.info(f"Pipeline done. {count} jobs upserted.")

    finally:
        await close_pool()
