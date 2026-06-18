from __future__ import annotations

from src.agent.memory.keywords import keywords_from_rewritten
from src.core.config import settings
from src.core.http_client import get_client
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
    append_work_mode_conditions,
    normalize_work_modes,
    work_mode_priority_order_sql,
)

logger = get_logger(__name__)

def _remove_location_terms(tokens: list[str], raw_location: str) -> list[str]:
    """Drop location words from FTS tsquery token list."""
    location_parts = {
        part.casefold()
        for part in str(raw_location or "").replace(",", " ").split()
        if part.strip()
    }
    if not location_parts:
        return tokens
    return [tok for tok in tokens if tok.casefold() not in location_parts]


def _strip_rewritten_context_tokens(tokens: list[str], parsed_query: dict) -> list[str]:
    """Remove location + work_mode tokens from rewritten-query fallback tokens."""
    raw_location = str(parsed_query.get("location") or "").strip()
    cleaned = _remove_location_terms(tokens, raw_location)

    drop_modes = {mode.casefold() for mode in normalize_work_modes(parsed_query.get("work_mode"))}
    if not drop_modes:
        return cleaned
    return [tok for tok in cleaned if tok.casefold() not in drop_modes]


def _to_or_tsquery_text(terms: list[str]) -> str:
    clauses: list[str] = []
    for term in terms:
        term = term.strip().replace('"', " ").strip()
        if not term:
            continue
        clauses.append(f'"{term}"' if " " in term else term)
    return " OR ".join(clauses)


def _build_fts_tsquery_text(parsed_query: dict, rewritten_query: str | None) -> tuple[str, str]:
    """Build FTS query text. Prefer parsed keywords; fallback to stripped rewritten query."""
    keywords = [str(k).strip() for k in (parsed_query.get("keywords") or []) if str(k).strip()]
    if keywords:
        return _to_or_tsquery_text(keywords), "parsed_keywords"

    rw = (rewritten_query or "").strip()
    if not rw:
        return "", "empty"

    tokens = keywords_from_rewritten(rw)
    tokens = _strip_rewritten_context_tokens(tokens, parsed_query)
    if tokens:
        return _to_or_tsquery_text(tokens), "rewritten_stripped"

    return "", "empty"


async def _get_embedding(text: str) -> list[float]:
    client = await get_client(settings.ollama_base_url, timeout=30)
    resp = await client.post(
        "/api/embed",
        json={"model": "bge-m3", "input": text},
    )
    resp.raise_for_status()
    data = resp.json()
    return data["embeddings"][0]


async def fts_search(
    parsed_query: dict,
    top_k: int = 30,
    *,
    rewritten_query: str | None = None,
) -> list[Job]:
    query_text, query_source = _build_fts_tsquery_text(parsed_query, rewritten_query)
    if not query_text:
        logger.warning(
            "fts_search called with no tsquery text (source=%s), returning empty list",
            query_source,
        )
        return []

    logger.info("FTS tsquery source=%s text=%r", query_source, query_text)

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

    idx = append_work_mode_conditions(
        parsed_query=parsed_query,
        conditions=conditions,
        params=params,
        idx=idx,
    )

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
        query_name="fts_search",
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

    work_modes = normalize_work_modes(parsed_query.get("work_mode"))
    order_by_parts = ["fts_score DESC", "updated_at DESC"]
    if work_modes:
        order_by_parts.insert(0, work_mode_priority_order_sql(work_modes))

    query = (
        f"SELECT {JOB_SELECT_COLUMNS}, "
        "ts_rank_cd("
        f"{TSVECTOR_SQL}, "
        "websearch_to_tsquery('public.vietnamese_unaccent', $1)"
        ") AS fts_score "
        "FROM jobs "
        f"WHERE {where_clause} "
        f"ORDER BY {', '.join(order_by_parts)} "
        f"LIMIT ${limit_idx}"
    )

    logger.info("FTS SQL query: %s | params=%s", query, params)

    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(query, *params)

    jobs = [Job.from_record(r) for r in rows]
    logger.info(
        "FTS search returned %d results for query=%r work_modes=%s",
        len(jobs),
        query_text,
        work_modes,
    )
    return jobs


async def vector_search(
    parsed_query: dict,
    top_k: int = 30,
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

    idx = append_work_mode_conditions(
        parsed_query=parsed_query,
        conditions=conditions,
        params=params,
        idx=idx,
    )

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

    work_modes = normalize_work_modes(parsed_query.get("work_mode"))
    order_by_parts = ["embedding <=> $1::vector", "updated_at DESC"]
    if work_modes:
        order_by_parts.insert(0, work_mode_priority_order_sql(work_modes))

    query = (
        f"SELECT {JOB_SELECT_COLUMNS}, "
        "1 - (embedding <=> $1::vector) AS vector_score "
        "FROM jobs "
        f"WHERE {where_clause} "
        f"ORDER BY {', '.join(order_by_parts)} "
        f"LIMIT ${limit_idx}"
    )

    logger.info("Vector SQL query: %s | params=%s", query, params)

    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(query, *params)

    jobs = [Job.from_record(r) for r in rows]
    logger.info(
        "Vector search returned %d results for query=%r work_modes=%s",
        len(jobs),
        query_text,
        work_modes,
    )
    return jobs
