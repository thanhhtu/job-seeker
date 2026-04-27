from __future__ import annotations

import asyncio

from src.agent.state import JobSearchState
from src.core.logger import get_logger
from src.retrieval.search import bm25_search, vector_search

logger = get_logger(__name__)


async def hybrid_search_node(state: JobSearchState) -> dict:
    parsed_query = state["parsed_query"]
    logger.info(f"Running hybrid search with parsed_query={parsed_query}")

    bm25_results, vector_results = await asyncio.gather(
        bm25_search(parsed_query, top_k=20),
        vector_search(parsed_query, top_k=20),
    )

    logger.info(
        f"Hybrid search done — BM25: {len(bm25_results)}, Vector: {len(vector_results)}"
    )
    return {
        "bm25_results": bm25_results,
        "vector_results": vector_results,
    }
