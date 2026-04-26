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
    pool = await get_pool()

    query = """
        INSERT INTO jobs (
            job_id, source, url, title, 
            company_name, company_url, company_id, company_size, company_industry, country,
            salary_raw, salary_min, salary_max, salary_currency, salary_negotiable,
            location_raw, locations,
            job_domains, job_level, description, requirements, skills, 
            experience_years_min, education, benefits, 
            work_mode, work_mode_days, overtime_policy, hiring_quantity, deadline,
            posted_date, crawled_date,
            created_at, updated_at,
            embedding
        )
        VALUES (
            $1,  $2,  $3,  $4,  
            $5,  $6,  $7,  $8,  $9,  $10,
            $11, $12, $13, $14, $15,
            $16, $17,
            $18, $19, $20, $21, $22,
            $23, $24, $25,
            $26, $27, $28, $29, $30,
            $31, $32,
            $33, $34,
            $35::vector
        )
        ON CONFLICT (source, job_id) DO UPDATE SET
            url                  = EXCLUDED.url,
            title                = EXCLUDED.title,
            company_name         = EXCLUDED.company_name,
            company_url          = EXCLUDED.company_url,
            company_id           = EXCLUDED.company_id,
            company_size         = EXCLUDED.company_size,
            company_industry     = EXCLUDED.company_industry,
            country              = EXCLUDED.country,
            salary_raw           = EXCLUDED.salary_raw,
            salary_min           = EXCLUDED.salary_min,
            salary_max           = EXCLUDED.salary_max,
            salary_currency      = EXCLUDED.salary_currency,
            salary_negotiable    = EXCLUDED.salary_negotiable,
            location_raw         = EXCLUDED.location_raw,
            locations            = EXCLUDED.locations,
            job_domains          = EXCLUDED.job_domains,
            job_level            = EXCLUDED.job_level,
            description          = EXCLUDED.description,
            requirements         = EXCLUDED.requirements,
            skills               = EXCLUDED.skills,
            experience_years_min = EXCLUDED.experience_years_min,
            education            = EXCLUDED.education,
            benefits             = EXCLUDED.benefits,
            work_mode            = EXCLUDED.work_mode,
            work_mode_days       = EXCLUDED.work_mode_days,
            overtime_policy      = EXCLUDED.overtime_policy,
            hiring_quantity      = EXCLUDED.hiring_quantity,
            deadline             = EXCLUDED.deadline,
            posted_date          = EXCLUDED.posted_date,
            crawled_date         = EXCLUDED.crawled_date,
            updated_at           = now(),
            embedding            = EXCLUDED.embedding
    """

    records = [
        (
            job.job_id, job.source, job.url, job.title,
            job.company_name, job.company_url, job.company_id, job.company_size, list(job.company_industry) if job.company_industry else [], job.country,
            job.salary_raw, job.salary_min, job.salary_max, job.salary_currency, job.salary_negotiable,
            job.location_raw, list(job.locations),
            list(job.job_domains), job.job_level, job.description, job.requirements, list(job.skills),
            job.experience_years_min, job.education, job.benefits,
            job.work_mode, job.work_mode_days, job.overtime_policy, job.hiring_quantity, job.deadline,
            job.posted_date, job.crawled_date,
            job.created_at, job.updated_at,
            _to_pgvector_literal(job.embedding)
        )
        for job in jobs
    ]

    async with pool.acquire() as conn:
        await conn.execute("SET search_path TO public")
        await conn.executemany(query, records)

    logger.info(f"Upserted {len(records)} jobs")
    return len(records)
