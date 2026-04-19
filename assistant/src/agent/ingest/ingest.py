import asyncio
import logging
from dataclasses import dataclass, field

from agent.db import JobRepository
from agent.models import Job

from .embedder import get_embedder_async
from .json_loader import load_all_jobs

logger = logging.getLogger(__name__)


@dataclass
class IngestResult:
    total: int = 0
    inserted: int = 0
    failed: int = 0
    errors: list[str] = field(default_factory=list)


def _compose_embedding_text(job: Job) -> str:
    parts = [
        job.title,
        f"Company: {job.company_name}",
    ]
    if job.skills:
        parts.append(f"Skills: {', '.join(job.skills)}")
    if job.job_domains:
        parts.append(f"Domains: {', '.join(job.job_domains)}")
    if job.locations:
        parts.append(f"Location: {', '.join(job.locations)}")
    parts.append(f"Description: {job.description}")
    if job.requirements:
        parts.append(f"Requirements: {job.requirements}")
    work_mode_str = f"Work mode: {job.work_mode}"
    if job.work_mode_days:
        work_mode_str += f" ({job.work_mode_days})"
    parts.append(work_mode_str)
    if job.job_level:
        parts.append(f"Level: {job.job_level}")
    if job.education:
        parts.append(f"Education: {job.education}")
    if job.experience_years_min > 0:
        parts.append(f"Experience: {job.experience_years_min}+ years")
    return "\n".join(parts)


async def ingest_jobs(
    batch_size: int = 100,
    itviec_path: str = "data/itviec_jobs_schema.json",
    topcv_path: str = "data/topcv_jobs_schema.json",
) -> IngestResult:
    """Ingest jobs from JSON files into PostgreSQL.

    Generates Mistral embeddings for each job and upserts into the database.
    """
    result = IngestResult()

    jobs = await load_all_jobs(itviec_path, topcv_path)
    result.total = len(jobs)

    embedder = await get_embedder_async()
    repo = JobRepository()

    for i in range(0, len(jobs), batch_size):
        batch = jobs[i : i + batch_size]

        texts = [_compose_embedding_text(job) for job in batch]
        try:
            loop = asyncio.get_event_loop()
            embeddings = await loop.run_in_executor(
                None, embedder.embed_documents, texts
            )
        except Exception as e:
            error_msg = f"Embedding API failed on batch {i // batch_size}: {e}"
            logger.warning(f"{error_msg}, falling back to per-job embedding")
            result.errors.append(error_msg)
            # Fall back to per-job embedding so no jobs are skipped
            for job in batch:
                try:
                    emb_text = _compose_embedding_text(job)
                    loop = asyncio.get_event_loop()
                    embedding = await loop.run_in_executor(
                        None, embedder.embed_query, emb_text
                    )
                    await repo.upsert(job, embedding)
                    result.inserted += 1
                except Exception as je:
                    error_msg = (
                        f"Failed to upsert job {job.job_id} ({job.source}): {je}"
                    )
                    logger.warning(error_msg)
                    result.errors.append(error_msg)
                    result.failed += 1
            continue

        for job, embedding in zip(batch, embeddings):
            try:
                await repo.upsert(job, embedding)
                result.inserted += 1
            except Exception as e:
                error_msg = f"Failed to upsert job {job.job_id} ({job.source}): {e}"
                logger.warning(error_msg)
                result.errors.append(error_msg)
                result.failed += 1

    return result
