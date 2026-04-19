import json
from typing import Sequence
from uuid import UUID

from agent.models.job import Job

from .pool import get_pool


class JobRepository:
    async def get_by_id(self, id: UUID) -> Job | None:
        pool = await get_pool()
        row = await pool.fetchrow(
            "SELECT * FROM jobs WHERE id = $1",
            id,
        )
        return Job.from_record(row) if row else None

    async def get_many(
        self,
        limit: int = 20,
        offset: int = 0,
        order_by: str = "posted_at DESC",
    ) -> Sequence[Job]:
        pool = await get_pool()
        rows = await pool.fetch(
            f"SELECT * FROM jobs ORDER BY {order_by} LIMIT $1 OFFSET $2",
            limit,
            offset,
        )
        return [Job.from_record(row) for row in rows]

    async def insert(self, job: Job, embedding: list[float] | None = None) -> Job:
        pool = await get_pool()
        async with pool.acquire() as conn:
            async with conn.transaction():
                row = await conn.fetchrow(
                    """
                    INSERT INTO jobs (
                        id, job_id, source, url, embedding, title, skills, job_domains,
                        description, requirements, work_mode, country, job_level,
                        education, salary_min, salary_max, salary_currency,
                        experience_years_min, deadline, posted_at, locations,
                        company_name, company_size, company_industry, work_mode_days,
                        overtime_policy, benefits, hiring_quantity, salary_negotiable
                    ) VALUES (
                        DEFAULT, $1, $2, $3,
                        $4::vector,
                        $5, $6, $7, $8, $9, $10, $11, $12, $13,
                        $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25,
                        $26, $27, $28
                    )
                    RETURNING *
                    """,
                    job.job_id,
                    job.source,
                    job.url,
                    json.dumps(embedding) if embedding else None,
                    job.title,
                    job.skills,
                    job.job_domains,
                    job.description,
                    job.requirements,
                    job.work_mode,
                    job.country,
                    job.job_level,
                    job.education,
                    job.salary_min,
                    job.salary_max,
                    job.salary_currency,
                    job.experience_years_min,
                    job.deadline,
                    job.posted_at,
                    job.locations,
                    job.company_name,
                    job.company_size,
                    job.company_industry,
                    job.work_mode_days,
                    job.overtime_policy,
                    job.benefits,
                    job.hiring_quantity,
                    job.salary_negotiable,
                )
                return Job.from_record(row)

    async def upsert(self, job: Job, embedding: list[float] | None = None) -> Job:
        pool = await get_pool()
        async with pool.acquire() as conn:
            async with conn.transaction():
                row = await conn.fetchrow(
                    """
                    INSERT INTO jobs (
                        id, job_id, source, url, embedding, title, skills, job_domains,
                        description, requirements, work_mode, country, job_level,
                        education, salary_min, salary_max, salary_currency,
                        experience_years_min, deadline, posted_at, locations,
                        company_name, company_size, company_industry, work_mode_days,
                        overtime_policy, benefits, hiring_quantity, salary_negotiable
                    ) VALUES (
                        DEFAULT, $1, $2, $3,
                        $4::vector,
                        $5, $6, $7, $8, $9, $10, $11, $12,
                        $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24,
                        $25, $26, $27, $28
                    )
                    ON CONFLICT (source, job_id) DO UPDATE SET
                        url = EXCLUDED.url,
                        title = EXCLUDED.title,
                        embedding = COALESCE(EXCLUDED.embedding::vector, jobs.embedding),
                        updated_at = now()
                    RETURNING *
                    """,
                    job.job_id,
                    job.source,
                    job.url,
                    json.dumps(embedding) if embedding else None,
                    job.title,
                    job.skills,
                    job.job_domains,
                    job.description,
                    job.requirements,
                    job.work_mode,
                    job.country,
                    job.job_level,
                    job.education,
                    job.salary_min,
                    job.salary_max,
                    job.salary_currency,
                    job.experience_years_min,
                    job.deadline,
                    job.posted_at,
                    job.locations,
                    job.company_name,
                    job.company_size,
                    job.company_industry,
                    job.work_mode_days,
                    job.overtime_policy,
                    job.benefits,
                    job.hiring_quantity,
                    job.salary_negotiable,
                )
                return Job.from_record(row)

    async def count(self) -> int:
        pool = await get_pool()
        return await pool.fetchval("SELECT COUNT(*) FROM jobs")

    async def delete_by_id(self, id: UUID) -> bool:
        pool = await get_pool()
        result = await pool.execute("DELETE FROM jobs WHERE id = $1", id)
        return "DELETE 1" in result

    async def search(
        self,
        query_text: str,
        embedding: list[float] | None = None,
        limit: int = 20,
        fts_weight: float = 1.0,
        vector_weight: float = 1.0,
    ) -> Sequence[Job]:
        """Hybrid search with 2-phase RRF fusion in Python.

        Phase 1: FTS search returns jobs ranked by ts_rank_cd with weighted fields
        Phase 2: Vector search returns jobs ranked by embedding similarity
        Phase 3: Python RRF (Reciprocal Rank Fusion) combines rankings

        RRF formula: score = Σ weight * 1/(k + rank)

        Args:
            query_text: Search query string
            embedding: Optional query embedding for vector search
            limit: Max results to return
            fts_weight: Weight multiplier for FTS scores (default 1.0)
            vector_weight: Weight multiplier for vector scores (default 1.0)
        """
        pool = await get_pool()
        k = 60  # RRF smoothing constant

        # ─── Phase 1: FTS Search ───────────────────────────────────────────────
        fts_rows = await pool.fetch(
            """
            WITH query_ts AS (
                SELECT websearch_to_tsquery('public.vietnamese_unaccent', $1) AS q
            ),
            fts_ranked AS (
                SELECT
                    j.id,
                    ts_rank_cd(
                        setweight(to_tsvector('public.vietnamese_unaccent', j.title), 'A') ||
                        setweight(to_tsvector('public.vietnamese_unaccent', j.company_name), 'B') ||
                        setweight(to_tsvector('public.vietnamese_unaccent', COALESCE(j.description, '')), 'C') ||
                        setweight(to_tsvector('public.vietnamese_unaccent', COALESCE(j.requirements, '')), 'C') ||
                        setweight(to_tsvector('public.vietnamese_unaccent', COALESCE(j.location_raw, '')), 'C') ||
                        setweight(to_tsvector('public.vietnamese_unaccent', COALESCE(array_to_string(j.locations, ' '), '')), 'C'),
                        (SELECT q FROM query_ts)
                    ) AS fts_rank,
                    ts_rank(
                        to_tsvector('public.vietnamese_unaccent', j.title),
                        (SELECT q FROM query_ts)
                    ) AS title_rank
                FROM jobs j
                WHERE
                    setweight(to_tsvector('public.vietnamese_unaccent', j.title), 'A') ||
                    setweight(to_tsvector('public.vietnamese_unaccent', j.company_name), 'B') ||
                    setweight(to_tsvector('public.vietnamese_unaccent', COALESCE(j.description, '')), 'C') ||
                    setweight(to_tsvector('public.vietnamese_unaccent', COALESCE(j.requirements, '')), 'C') ||
                    setweight(to_tsvector('public.vietnamese_unaccent', COALESCE(j.location_raw, '')), 'C') ||
                    setweight(to_tsvector('public.vietnamese_unaccent', COALESCE(array_to_string(j.locations, ' '), '')), 'C')
                    @@ (SELECT q FROM query_ts)
            )
            SELECT id, fts_rank, title_rank, 1 as fts_source
            FROM fts_ranked
            ORDER BY title_rank DESC, fts_rank DESC
            LIMIT $2
            """,
            query_text,
            limit * 3,  # Fetch more for better fusion coverage
        )

        # Build FTS rank dict: job_id -> rank (1-based)
        fts_ranks: dict[str, int] = {}
        for rank, row in enumerate(fts_rows, 1):
            fts_ranks[str(row["id"])] = rank

        # ─── Phase 2: Vector Search (if embedding provided) ───────────────────
        vector_ranks: dict[str, int] = {}
        if embedding is not None:
            vector_rows = await pool.fetch(
                """
                SELECT id, row_number() OVER (ORDER BY embedding <=> $1::vector) AS vec_rank
                FROM jobs
                WHERE embedding IS NOT NULL
                  AND embedding <=> $1::vector < 0.6
                LIMIT $2
                """,
                json.dumps(embedding),
                limit * 3,
            )
            for rank, row in enumerate(vector_rows, 1):
                vector_ranks[str(row["id"])] = rank

        # ─── Phase 3: Python RRF Fusion ────────────────────────────────────────
        # Collect all job IDs from both result sets
        all_job_ids = set(fts_ranks.keys()) | set(vector_ranks.keys())

        # Calculate RRF scores
        rrf_scores: dict[str, float] = {}
        for job_id in all_job_ids:
            score = 0.0
            if job_id in fts_ranks:
                score += fts_weight * (1.0 / (k + fts_ranks[job_id]))
            if job_id in vector_ranks:
                score += vector_weight * (1.0 / (k + vector_ranks[job_id]))
            rrf_scores[job_id] = score

        # Sort by RRF score descending
        sorted_job_ids = sorted(rrf_scores.keys(), key=lambda jid: rrf_scores[jid], reverse=True)

        # Fetch full job records for top results
        if not sorted_job_ids:
            return []

        top_job_ids = sorted_job_ids[:limit]
        placeholders = ",".join(f"${i}" for i in range(1, len(top_job_ids) + 1))
        rows = await pool.fetch(
            f"SELECT * FROM jobs WHERE id IN ({placeholders})",
            *[job_id for job_id in top_job_ids],
        )

        # Preserve RRF ordering
        rows_dict = {str(row["id"]): row for row in rows}
        ordered_rows = [rows_dict[jid] for jid in top_job_ids if jid in rows_dict]

        return [Job.from_record(row) for row in ordered_rows]
