from __future__ import annotations

from src.core.config import settings
from src.core.http_client import get_client
from src.core.logger import get_logger

logger = get_logger(__name__)


async def rerank(query: str, documents: list[str]) -> list[float]:
    if not documents:
        return []

    payload = {"query": query, "documents": documents}

    client = await get_client(settings.reranker_url, timeout=settings.reranker_timeout)
    resp = await client.post("/rerank", json=payload)
    resp.raise_for_status()
    data = resp.json()

    scores: list[float] = data["scores"]
    logger.info(f"Reranker scored {len(scores)} documents")
    return scores
