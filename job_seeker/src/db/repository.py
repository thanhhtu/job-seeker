import asyncpg
from src.db.client import get_pool
from src.core.logger import get_logger
from src.models.job_schema import Job

logger = get_logger(__name__)


def _to_pgvector_literal(embedding: list[float] | None) -> str | None:
    if not embedding:
        return None
    return "[" + ",".join(str(float(x)) for x in embedding) + "]"


async def upsert_jobs(jobs: list[Job]) -> int:
    """
    Insert hoặc update jobs vào DB theo schema thực tế.
    Conflict key: (source, job_id)
    """
    pool = await get_pool()

    query = """
        INSERT INTO jobs (
            source, job_id, url, company_url, company_id,
            title, skills, job_domains, description, requirements,
            work_mode, country, job_level, education,
            salary_min, salary_max, salary_currency, salary_raw, salary_negotiable,
            experience_years_min,
            posted_date, crawled_date,
            locations, location_raw,
            company_name, company_size, company_industry,
            work_mode_days, overtime_policy, benefits,
            hiring_quantity, deadline,
            embedding
        )
        VALUES (
            $1,  $2,  $3,  $4,  $5,
            $6,  $7,  $8,  $9,  $10,
            $11, $12, $13, $14,
            $15, $16, $17, $18, $19,
            $20,
            $21, $22,
            $23, $24,
            $25, $26, $27,
            $28, $29, $30,
            $31, $32,
            $33::vector
        )
        ON CONFLICT (source, job_id) DO UPDATE SET
            url                  = EXCLUDED.url,
            company_url          = EXCLUDED.company_url,
            company_id           = EXCLUDED.company_id,
            title                = EXCLUDED.title,
            skills               = EXCLUDED.skills,
            job_domains          = EXCLUDED.job_domains,
            description          = EXCLUDED.description,
            requirements         = EXCLUDED.requirements,
            work_mode            = EXCLUDED.work_mode,
            country              = EXCLUDED.country,
            job_level            = EXCLUDED.job_level,
            education            = EXCLUDED.education,
            salary_min           = EXCLUDED.salary_min,
            salary_max           = EXCLUDED.salary_max,
            salary_currency      = EXCLUDED.salary_currency,
            salary_raw           = EXCLUDED.salary_raw,
            salary_negotiable    = EXCLUDED.salary_negotiable,
            experience_years_min = EXCLUDED.experience_years_min,
            posted_date          = EXCLUDED.posted_date,
            crawled_date         = EXCLUDED.crawled_date,
            locations            = EXCLUDED.locations,
            location_raw         = EXCLUDED.location_raw,
            company_name         = EXCLUDED.company_name,
            company_size         = EXCLUDED.company_size,
            company_industry     = EXCLUDED.company_industry,
            work_mode_days       = EXCLUDED.work_mode_days,
            overtime_policy      = EXCLUDED.overtime_policy,
            benefits             = EXCLUDED.benefits,
            hiring_quantity      = EXCLUDED.hiring_quantity,
            deadline             = EXCLUDED.deadline,
            embedding            = EXCLUDED.embedding,
            updated_at           = now()
    """

    records = [
        (
            job.source,
            job.job_id,
            job.url,
            job.company_url,
            job.company_id,
            job.title,
            list(job.skills),
            list(job.job_domains),
            job.description,
            job.requirements,
            job.work_mode,
            job.country,
            job.job_level,
            job.education,
            job.salary_min,
            job.salary_max,
            job.salary_currency,
            job.salary_raw,
            job.salary_negotiable,
            job.experience_years_min,
            job.posted_date,
            job.crawled_date,
            list(job.locations),
            job.location_raw,
            job.company_name,
            job.company_size,
            list(job.company_industry) if job.company_industry else [],
            job.work_mode_days,
            job.overtime_policy,
            job.benefits,
            job.hiring_quantity,
            job.deadline,
            _to_pgvector_literal(job.embedding),
        )
        for job in jobs
    ]

    async with pool.acquire() as conn:
        # Register vector type để asyncpg serialize list[float] -> vector
        await conn.execute("SET search_path TO public")
        await conn.executemany(query, records)

    logger.info(f"Upserted {len(records)} jobs")
    return len(records)
