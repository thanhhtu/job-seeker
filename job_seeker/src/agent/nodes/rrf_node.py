"""
Weighted Reciprocal Rank Fusion (RRF): Combine results from FTS and Vector search using weighted RRF.

Formula: score(d) = Σ  w_i / (k + rank_i(d))
    k = 60  (default value from the paper "Reciprocal Rank Fusion outperforms Condorcet and individual Rank Learning Methods")
    w_i start from base constants and are adjusted per query.

Results are sorted in descending order by RRF score.
"""
from __future__ import annotations

from src.agent.states.state import JobSearchState
from src.core.logger import get_logger
from src.models.job_schema import Job

logger = get_logger(__name__)

RRF_K = 60                  # Constant from the original paper
TOP_N = 30                  # Number of results kept after RRF to pass to reranker
FTS_WEIGHT_BASE = 1.1
VECTOR_WEIGHT_BASE = 0.9
MAX_PER_LIST = 30


def _job_key(job: Job) -> str:
    """Unique key for each job — prefer UUID, fallback to (source, job_id)."""
    if job.id is not None:
        return str(job.id)
    return f"{job.source}::{job.job_id}"


def _dedupe_jobs(jobs: list[Job]) -> list[Job]:
    seen: set[str] = set()
    out: list[Job] = []
    for job in jobs:
        key = _job_key(job)
        if key in seen:
            continue
        seen.add(key)
        out.append(job)
    return out


def _resolve_weights(state: JobSearchState) -> tuple[float, float]:
    parsed = state.get("parsed_query") or {}
    rewritten = str(state.get("rewritten_query") or "").strip()

    has_hard_filters = any(
        parsed.get(k) not in (None, "", [])
        for k in (
            "location",
            "work_mode",
            "job_level",
            "salary_min",
            "salary_max",
            "salary_currency",
            "candidate_experience_years",
            "job_experience_min",
            "job_experience_max",
            "skills",
        )
    )
    has_rich_rewrite = len(rewritten.split()) >= 8

    fts_w = FTS_WEIGHT_BASE + (0.2 if has_hard_filters else 0.0)
    vector_w = VECTOR_WEIGHT_BASE + (0.2 if has_rich_rewrite else 0.0)
    return fts_w, vector_w


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
    fts_results = _dedupe_jobs((state.get("fts_results", []) or [])[:MAX_PER_LIST])
    vector_results = _dedupe_jobs((state.get("vector_results", []) or [])[:MAX_PER_LIST])
    fts_weight, vector_weight = _resolve_weights(state)

    logger.info(
        f"RRF merging — FTS: {len(fts_results)} (w={fts_weight:.2f}), "
        f"Vector: {len(vector_results)} (w={vector_weight:.2f})"
    )

    merged = reciprocal_rank_fusion(
        [
            (fts_results, fts_weight),
            (vector_results, vector_weight),
        ]
    )
    rrf_results = merged[:TOP_N]

    logger.info(f"RRF produced {len(rrf_results)} candidates")
    return {"rrf_results": rrf_results}
