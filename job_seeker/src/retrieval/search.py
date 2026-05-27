from __future__ import annotations

from collections.abc import Sequence

from src.agent.memory.keywords import keywords_from_rewritten
from src.core.config import settings
from src.core.logger import get_logger
from src.db.client import get_pool
from src.models.job_schema import Job
from src.retrieval._filters import (
    JOB_SELECT_COLUMNS,
    TSVECTOR_SQL,
    append_experience_conditions,
    append_extra_filters,
    append_location_conditions,
    append_salary_conditions,
    append_skills_conditions,
    normalize_work_mode,
)

logger = get_logger(__name__)


def _sql_literal(value: object) -> str:
    """Render a Python value into a SQL literal for debug logs only."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        items = ", ".join(_sql_literal(item) for item in value)
        return f"ARRAY[{items}]"
    return "'" + str(value).replace("'", "''") + "'"


def _render_sql_with_params(query: str, params: list[object]) -> str:
    """Best-effort replacement of $1..$N placeholders for readable debug SQL."""
    rendered = query
    for index in range(len(params), 0, -1):
        rendered = rendered.replace(f"${index}", _sql_literal(params[index - 1]))
    return rendered


def _remove_location_terms(tokens: list[str], raw_location: str) -> list[str]:
    """Drop location words from BM25 tsquery token list."""
    location_parts = {
        part.casefold()
        for part in str(raw_location or "").replace(",", " ").split()
        if part.strip()
    }
    if not location_parts:
        return tokens
    cleaned = [tok for tok in tokens if tok.casefold() not in location_parts]
    return cleaned


async def _get_embedding(text: str) -> list[float]:
    import httpx

    async with httpx.AsyncClient(base_url=settings.ollama_base_url, timeout=30) as client:
        resp = await client.post(
            "/api/embed",
            json={"model": "bge-m3", "input": text},
        )
        resp.raise_for_status()
        data = resp.json()
        return data["embeddings"][0]


async def bm25_search(
    parsed_query: dict,
    top_k: int = 20,
    *,
    rewritten_query: str | None = None,
) -> list[Job]:
    keywords: list[str] = list(parsed_query.get("keywords") or [])
    raw_location = str(parsed_query.get("location") or "").strip()
    rw = (rewritten_query or "").strip()
    if rw:
        rw_kws = keywords_from_rewritten(rw)
        if rw_kws:
            keywords = rw_kws
    keywords = _remove_location_terms(keywords, raw_location)
    query_text = " ".join(keywords).strip()
    if not query_text:
        logger.warning("bm25_search called with no keywords, returning empty list")
        return []

    conditions: list[str] = [
        f"{TSVECTOR_SQL} @@ websearch_to_tsquery('public.vietnamese_unaccent', $1)"
    ]
    params: list = [query_text]
    idx = 2

    idx = append_location_conditions(
        parsed_query=parsed_query,
        conditions=conditions,
        params=params,
        idx=idx,
    )

    if level := parsed_query.get("job_level"):
        conditions.append(f"lower(coalesce(job_level, '')) = ${idx}")
        params.append(str(level).strip().lower())
        idx += 1

    if mode := parsed_query.get("work_mode"):
        norm_mode = normalize_work_mode(mode)
        if norm_mode:
            conditions.append(f"lower(coalesce(work_mode, '')) = ${idx}")
            params.append(norm_mode)
            idx += 1

    idx = append_experience_conditions(
        parsed_query=parsed_query,
        conditions=conditions,
        params=params,
        idx=idx,
    )

    idx = append_salary_conditions(
        parsed_query=parsed_query,
        conditions=conditions,
        params=params,
        idx=idx,
        query_name="bm25_search",
    )

    idx = append_skills_conditions(
        parsed_query=parsed_query,
        conditions=conditions,
        params=params,
        idx=idx,
    )

    idx = append_extra_filters(
        parsed_query=parsed_query,
        conditions=conditions,
        params=params,
        idx=idx,
    )

    where_clause = " AND ".join(conditions)
    limit_idx = idx
    params.append(int(top_k))

    query = (
        f"SELECT {JOB_SELECT_COLUMNS}, "
        "ts_rank_cd("
        f"{TSVECTOR_SQL}, "
        "websearch_to_tsquery('public.vietnamese_unaccent', $1)"
        ") AS bm25_score "
        "FROM jobs "
        f"WHERE {where_clause} "
        "ORDER BY bm25_score DESC, updated_at DESC "
        f"LIMIT ${limit_idx}"
    )

    logger.info("BM25 SQL query: %s | params=%s", query, params)
    logger.info("BM25 SQL rendered: %s", _render_sql_with_params(query, params))

    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(query, *params)

    jobs = [Job.from_record(r) for r in rows]
    logger.info("BM25 search returned %d results for query=%r", len(jobs), query_text)
    return jobs


async def vector_search(
    parsed_query: dict,
    top_k: int = 20,
    *,
    rewritten_query: str | None = None,
    conversation_summary: str | None = None,
) -> list[Job]:
    rw = (rewritten_query or "").strip()
    if rw:
        query_text = rw
    else:
        keyword_str = " ".join(parsed_query.get("keywords", []))
        location_str = str(parsed_query.get("location") or "")
        query_text = f"{keyword_str} {location_str}".strip()
    summary = (conversation_summary or "").strip()
    if summary:
        query_text = f"{summary}\n{query_text}".strip()

    if not query_text:
        logger.warning("vector_search: empty query text, returning empty list")
        return []

    embedding = await _get_embedding(query_text)
    vector_literal = "[" + ",".join(str(float(v)) for v in embedding) + "]"

    conditions: list[str] = ["embedding IS NOT NULL"]
    params: list = [vector_literal]
    idx = 2

    idx = append_location_conditions(
        parsed_query=parsed_query,
        conditions=conditions,
        params=params,
        idx=idx,
    )

    if level := parsed_query.get("job_level"):
        conditions.append(f"lower(coalesce(job_level, '')) = ${idx}")
        params.append(str(level).strip().lower())
        idx += 1

    if mode := parsed_query.get("work_mode"):
        norm_mode = normalize_work_mode(mode)
        if norm_mode:
            conditions.append(f"lower(coalesce(work_mode, '')) = ${idx}")
            params.append(norm_mode)
            idx += 1

    idx = append_experience_conditions(
        parsed_query=parsed_query,
        conditions=conditions,
        params=params,
        idx=idx,
    )

    idx = append_salary_conditions(
        parsed_query=parsed_query,
        conditions=conditions,
        params=params,
        idx=idx,
        query_name="vector_search",
    )

    idx = append_skills_conditions(
        parsed_query=parsed_query,
        conditions=conditions,
        params=params,
        idx=idx,
    )

    idx = append_extra_filters(
        parsed_query=parsed_query,
        conditions=conditions,
        params=params,
        idx=idx,
    )

    where_clause = " AND ".join(conditions)
    limit_idx = idx
    params.append(int(top_k))

    query = (
        f"SELECT {JOB_SELECT_COLUMNS}, "
        "1 - (embedding <=> $1::vector) AS vector_score "
        "FROM jobs "
        f"WHERE {where_clause} "
        "ORDER BY embedding <=> $1::vector, updated_at DESC "
        f"LIMIT ${limit_idx}"
    )

    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(query, *params)

    jobs = [Job.from_record(r) for r in rows]
    logger.info("Vector search returned %d results for query=%r", len(jobs), query_text)
    return jobs
