from __future__ import annotations

import asyncio
import heapq
import math

from src.agent.states.state import JobSearchState
from src.core.config import settings
from src.core.logger import get_logger
from src.models.job_schema import Job
from src.retrieval.reranker import rerank

logger = get_logger(__name__)


def _job_to_text(job: Job) -> str:
    max_chars = settings.reranker_desc_max_chars
    parts = list(filter(None, [job.title, job.company_name]))
    if job.description:
        parts.append(job.description[:max_chars])
    if job.skills:
        parts.append("Skills: " + ", ".join(job.skills))
    if job.locations:
        parts.append("Location: " + ", ".join(job.locations))
    return " | ".join(filter(None, parts))


def _fallback_results(rrf_results: list[Job]) -> dict:
    return {"reranked_results": rrf_results[: settings.reranker_top_k]}


async def reranker_node(state: JobSearchState) -> dict:
    rrf_results: list[Job] = state.get("rrf_results") or []
    query: str = (
        state.get("rewritten_query") or state.get("raw_query") or ""
    ).strip()

    if not rrf_results:
        logger.warning("reranker_node: no RRF results to rerank")
        return {"reranked_results": []}

    if not query:
        logger.warning(
            "reranker_node: empty query — skipping rerank, returning RRF order"
        )
        return _fallback_results(rrf_results)

    documents = [_job_to_text(job) for job in rrf_results]
    logger.info("Reranking %d docs (query=%r)", len(documents), query)

    try:
        scores = await asyncio.wait_for(
            rerank(query=query, documents=documents),
            timeout=settings.reranker_timeout,
        )
    except asyncio.TimeoutError:
        logger.error(
            "reranker_node: rerank timed out after %ds — returning RRF order",
            settings.reranker_timeout,
        )
        return _fallback_results(rrf_results)
    except Exception:
        logger.exception("reranker_node: rerank failed — returning RRF order")
        return _fallback_results(rrf_results)

    if len(scores) != len(rrf_results):
        logger.error(
            "reranker_node: score count (%d) != result count (%d) — returning RRF order",
            len(scores),
            len(rrf_results),
        )
        return _fallback_results(rrf_results)

    valid_scores: list[float] = []
    for s in scores:
        try:
            f = float(s)
            valid_scores.append(f if math.isfinite(f) else float("-inf"))
        except (TypeError, ValueError):
            valid_scores.append(float("-inf"))

    finite_scores = [s for s in valid_scores if math.isfinite(s)]
    n_invalid = len(valid_scores) - len(finite_scores)
    if n_invalid:
        logger.warning(
            "reranker_node: %d/%d invalid reranker scores",
            n_invalid,
            len(valid_scores),
        )
    if finite_scores:
        logger.info(
            "Reranker score distribution: min=%.4f, max=%.4f, mean=%.4f",
            min(finite_scores),
            max(finite_scores),
            sum(finite_scores) / len(finite_scores),
        )
    else:
        logger.warning(
            "reranker_node: all scores invalid; selection will fall back to RRF order for ties"
        )

    top_k = min(settings.reranker_top_k, len(rrf_results))
    top_scored = heapq.nlargest(
        top_k,
        enumerate(valid_scores),
        key=lambda item: (item[1], -item[0]),
    )
    reranked_results = [rrf_results[idx] for idx, _ in top_scored]

    logger.info("Reranker selected top %d jobs", len(reranked_results))
    return {"reranked_results": reranked_results}
