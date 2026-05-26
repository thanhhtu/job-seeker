from __future__ import annotations

import asyncio

from src.agent.state import JobSearchState
from src.core.logger import get_logger
from src.retrieval.search import bm25_search, vector_search

logger = get_logger(__name__)

SEARCH_TOP_K = 30


async def hybrid_search_node(state: JobSearchState) -> dict:
    parsed_query = state.get("parsed_query") or {}
    rewritten = (state.get("rewritten_query") or "").strip()
    summary = (state.get("conversation_summary") or "").strip()
    logger.info(
        "Running hybrid search parsed_keys=%s rewritten=%r",
        list(parsed_query.keys()),
        rewritten[:80] if rewritten else "",
    )

    bm25_result, vector_result = await asyncio.gather(
        bm25_search(parsed_query, top_k=SEARCH_TOP_K, rewritten_query=rewritten or None),
        vector_search(
            parsed_query,
            top_k=SEARCH_TOP_K,
            rewritten_query=rewritten or None,
            conversation_summary=summary or None,
        ),
        return_exceptions=True,
    )

    bm25_results = bm25_result if isinstance(bm25_result, list) else []
    vector_results = vector_result if isinstance(vector_result, list) else []
    if isinstance(bm25_result, Exception):
        logger.exception("BM25 search failed", exc_info=bm25_result)
    if isinstance(vector_result, Exception):
        logger.exception("Vector search failed", exc_info=vector_result)

    logger.info(
        f"Hybrid search done — BM25: {len(bm25_results)}, Vector: {len(vector_results)}"
    )
    return {
        "bm25_results": bm25_results,
        "vector_results": vector_results,
    }
