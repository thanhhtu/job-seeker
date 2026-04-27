import asyncio
import threading
import httpx

from src.core.config import settings
from src.core.logger import get_logger
from src.models.job_schema import Job

logger = get_logger(__name__)

BGE_MODEL = "bge-m3"
EMBED_BATCH_SIZE = 32
OLLAMA_BASE_URL = settings.ollama_base_url

_client_instance: httpx.AsyncClient | None = None
_client_lock = threading.Lock()


def get_http_client() -> httpx.AsyncClient:
    global _client_instance
    if _client_instance is None:
        with _client_lock:
            if _client_instance is None:
                _client_instance = httpx.AsyncClient(
                    base_url=OLLAMA_BASE_URL,
                    timeout=60.0,
                )
    return _client_instance


async def _embed_texts(texts: list[str] | str) -> list[list[float]]:
    client = get_http_client()
    response = await client.post(
        "/api/embed",
        json={
            "model": BGE_MODEL,
            "input": texts if isinstance(texts, list) else [texts],
        }
    )
    response.raise_for_status()
    return response.json()["embeddings"]


async def embed_query_async(query: str) -> list[float]:
    embeddings = await _embed_texts(query)
    return embeddings[0]


def _build_embed_text(job: Job) -> str:
    """
    Combine important fields into a single string for embedding.
    Title + skills + job_domains are repeated to increase their weight.
    """
    parts = [
        job.title,
        job.title,  # double to increase weight
        " ".join(job.skills),
        " ".join(job.job_domains),
        job.description[:1000] if job.description else "",
        job.requirements[:500] if job.requirements else "",
    ]
    return " | ".join(p for p in parts if p.strip())


async def embed_jobs(jobs: list[dict]) -> list[Job]:
    """
    Receive a list[dict] raw from JSON loader,
    parse into Job objects, embed, and return list[Job] with embeddings set.
    """
    # Parse dict -> Job
    job_objects: list[Job] = [Job.from_json(j) for j in jobs]

    # Build texts to embed
    texts = [_build_embed_text(job) for job in job_objects]

    # Batch embed
    all_embeddings: list[list[float]] = []
    for i in range(0, len(texts), EMBED_BATCH_SIZE):
        batch = texts[i : i + EMBED_BATCH_SIZE]
        logger.info(f"Embedding batch {i // EMBED_BATCH_SIZE + 1} ({len(batch)} jobs)...")
        embeddings = await _embed_texts(batch)
        all_embeddings.extend(embeddings)

    # Assign embedding to each Job
    for job, emb in zip(job_objects, all_embeddings):
        job.embedding = emb

    logger.info(f"Embedded {len(job_objects)} jobs successfully")
    return job_objects


async def close_client():
    """Call on app shutdown to close the httpx client."""
    global _client_instance
    if _client_instance:
        await _client_instance.aclose()
        _client_instance = None
