from __future__ import annotations

from src.agent.state import JobSearchState
from src.core.logger import get_logger
from src.models.job_schema import Job
from src.retrieval.reranker import rerank

logger = get_logger(__name__)

TOP_K = 10  # Number of top results to keep after reranking


def _job_to_text(job: Job) -> str:
    parts = [job.title, job.company_name]
    if job.description:
        parts.append(job.description[:500])
    if job.skills:
        parts.append("Skills: " + ", ".join(job.skills))
    if job.locations:
        parts.append("Location: " + ", ".join(job.locations))
    return " | ".join(filter(None, parts))


async def reranker_node(state: JobSearchState) -> dict:
    rrf_results: list[Job] = state.get("rrf_results", [])
    raw_query: str = state.get("raw_query", "")

    if not rrf_results:
        logger.warning("reranker_node: no RRF results to rerank")
        return {"reranked_results": []}

    documents = [_job_to_text(job) for job in rrf_results]

    logger.info(
        f"Sending {len(documents)} documents to BGE Reranker "
        f"(query={raw_query!r})"
    )

    scores = await rerank(query=raw_query, documents=documents)

    # Zip scores with jobs, sort by score, and take top K
    scored = sorted(
        zip(scores, rrf_results),
        key=lambda x: x[0],
        reverse=True,
    )

    reranked_results = [job for _, job in scored[:TOP_K]]

    logger.info(f"Reranker selected top {len(reranked_results)} jobs")
    return {"reranked_results": reranked_results}
