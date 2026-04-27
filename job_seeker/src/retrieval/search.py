"""
Retrieval: BM25 (PostgreSQL tsvector) + Vector (pgvector) search.
"""
from __future__ import annotations

import asyncpg

from src.core.config import settings
from src.core.logger import get_logger
from src.db.client import get_pool
from src.models.job_schema import Job

logger = get_logger(__name__)

async def _get_embedding(text: str) -> list[float]:
    import httpx

    async with httpx.AsyncClient(base_url=settings.ollama_base_url, timeout=30) as client:
        resp = await client.post(
            "/api/embed",
            json={"model": "bge-m3", "input": text},
        )
        resp.raise_for_status()
        data = resp.json()
        # Ollama returns {"embeddings": [[...]]}
        return data["embeddings"][0]

async def bm25_search(parsed_query: dict, top_k: int = 20) -> list[Job]:
    keywords: list[str] = parsed_query.get("keywords", [])
    if not keywords:
        logger.warning("bm25_search called with no keywords, returning empty list")
        return []

    ts_query = " & ".join(keywords)

    conditions: list[str] = [
        "to_tsvector('english', coalesce(title,'') || ' ' || coalesce(description,'') || ' ' || coalesce(requirements,'')) @@ to_tsquery('english', $1)"
    ]
    params: list = [ts_query]
    idx = 2  

    if loc := parsed_query.get("location"):
        conditions.append(f"${idx} = ANY(locations)")
        params.append(loc)
        idx += 1

    if level := parsed_query.get("job_level"):
        conditions.append(f"job_level = ${idx}")
        params.append(level)
        idx += 1

    if mode := parsed_query.get("work_mode"):
        conditions.append(f"work_mode = ${idx}")
        params.append(mode)
        idx += 1

    if (exp := parsed_query.get("experience_years")) is not None:
        conditions.append(f"experience_years_min <= ${idx}")
        params.append(int(exp))
        idx += 1

    if (sal := parsed_query.get("salary_min")) is not None:
        conditions.append(f"(salary_max IS NULL OR salary_max >= ${idx})")
        params.append(float(sal))
        idx += 1

    where_clause = " AND ".join(conditions)

    query = f"""
        SELECT *, ts_rank(
            to_tsvector('english', coalesce(title,'') || ' ' || coalesce(description,'') || ' ' || coalesce(requirements,'')),
            to_tsquery('english', $1)
        ) AS bm25_score
        FROM jobs
        WHERE {where_clause}
        ORDER BY bm25_score DESC
        LIMIT {top_k}
    """

    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("SET search_path TO public")
        rows = await conn.fetch(query, *params)

    jobs = [Job.from_record(r) for r in rows]
    logger.info(f"BM25 search returned {len(jobs)} results for keywords={keywords}")
    return jobs

async def vector_search(parsed_query: dict, top_k: int = 20) -> list[Job]:
    keyword_str = " ".join(parsed_query.get("keywords", []))
    location_str = parsed_query.get("location", "")
    query_text = f"{keyword_str} {location_str}".strip()

    if not query_text:
        logger.warning("vector_search: empty query text, returning empty list")
        return []

    embedding = await _get_embedding(query_text)
    vector_literal = "[" + ",".join(str(float(v)) for v in embedding) + "]"

    conditions: list[str] = ["embedding IS NOT NULL"]
    params: list = [vector_literal]
    idx = 2

    if level := parsed_query.get("job_level"):
        conditions.append(f"job_level = ${idx}")
        params.append(level)
        idx += 1

    if mode := parsed_query.get("work_mode"):
        conditions.append(f"work_mode = ${idx}")
        params.append(mode)
        idx += 1

    if (exp := parsed_query.get("experience_years")) is not None:
        conditions.append(f"experience_years_min <= ${idx}")
        params.append(int(exp))
        idx += 1

    if (sal := parsed_query.get("salary_min")) is not None:
        conditions.append(f"(salary_max IS NULL OR salary_max >= ${idx})")
        params.append(float(sal))
        idx += 1

    where_clause = " AND ".join(conditions)

    query = f"""
        SELECT *, 1 - (embedding <=> $1::vector) AS vector_score
        FROM jobs
        WHERE {where_clause}
        ORDER BY embedding <=> $1::vector
        LIMIT {top_k}
    """

    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("SET search_path TO public")
        rows = await conn.fetch(query, *params)

    jobs = [Job.from_record(r) for r in rows]
    logger.info(f"Vector search returned {len(jobs)} results for: '{query_text}'")
    return jobs
