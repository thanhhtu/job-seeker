"""
Weighted Reciprocal Rank Fusion (RRF): Combine results from BM25 and Vector search using weighted RRF.

Formula: score(d) = Σ  w_i / (k + rank_i(d))
    k = 60  (default value from the paper "Reciprocal Rank Fusion outperforms Condorcet and individual Rank Learning Methods")
    w_i are defined as constants in this file (BM25_WEIGHT, VECTOR_WEIGHT)

Results are sorted in descending order by RRF score.
"""
from __future__ import annotations

from uuid import UUID

from src.agent.state import JobSearchState
from src.core.logger import get_logger
from src.models.job_schema import Job

logger = get_logger(__name__)

RRF_K = 60  # Constant from the original paper
TOP_N = 30  # Number of results kept after RRF to pass to reranker
BM25_WEIGHT = 1.0
VECTOR_WEIGHT = 1.0


def _job_key(job: Job) -> str:
    """Unique key for each job — prefer UUID, fallback to (source, job_id)."""
    if job.id is not None:
        return str(job.id)
    return f"{job.source}::{job.job_id}"


def reciprocal_rank_fusion(
    weighted_ranked_lists: list[tuple[list[Job], float]],
    k: int = RRF_K,
) -> list[Job]:
    """
    Take multiple ranked lists and return a merged list using RRF.
    """
    scores: dict[str, float] = {}
    job_map: dict[str, Job] = {}  # key → Job object

    for ranked_list, weight in weighted_ranked_lists:
        if weight <= 0:
            continue
        for rank, job in enumerate(ranked_list, start=1):
            key = _job_key(job)
            scores[key] = scores.get(key, 0.0) + (weight / (k + rank))
            if key not in job_map:
                job_map[key] = job

    # Sort by RRF score in descending order
    sorted_keys = sorted(scores, key=lambda k: scores[k], reverse=True)
    return [job_map[key] for key in sorted_keys]


def rrf_node(state: JobSearchState) -> dict:
    bm25_results = state.get("bm25_results", [])
    vector_results = state.get("vector_results", [])

    logger.info(
        f"RRF merging — BM25: {len(bm25_results)} (w={BM25_WEIGHT}), "
        f"Vector: {len(vector_results)} (w={VECTOR_WEIGHT})"
    )

    merged = reciprocal_rank_fusion(
        [
            (bm25_results, BM25_WEIGHT),
            (vector_results, VECTOR_WEIGHT),
        ]
    )
    rrf_results = merged[:TOP_N]

    logger.info(f"RRF produced {len(rrf_results)} candidates")
    return {"rrf_results": rrf_results}
