import asyncio
import threading
import httpx

from src.core.config import settings
from src.core.logger import get_logger
from src.models.job_schema import Job

logger = get_logger(__name__)

BGE_MODEL = "bge-m3"
EMBED_BATCH_SIZE = 32
OLLAMA_BASE_URL = settings.ollama_base_url  # ví dụ: "http://ollama:11434"

_client_instance: httpx.AsyncClient | None = None
_client_lock = threading.Lock()


def get_http_client() -> httpx.AsyncClient:
    global _client_instance
    if _client_instance is None:
        with _client_lock:
            if _client_instance is None:
                _client_instance = httpx.AsyncClient(
                    base_url=OLLAMA_BASE_URL,
                    timeout=60.0,  # bge-m3 có thể chậm hơn
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
    Ghép các field quan trọng thành 1 chuỗi để embed.
    Title + skills + job_domains có trọng số cao hơn nên lặp lại.
    """
    parts = [
        job.title,
        job.title,  # nhân đôi để tăng weight
        " ".join(job.skills),
        " ".join(job.job_domains),
        job.description[:1000] if job.description else "",
        job.requirements[:500] if job.requirements else "",
    ]
    return " | ".join(p for p in parts if p.strip())


async def embed_jobs(jobs: list[dict]) -> list[Job]:
    """
    Nhận list[dict] raw từ JSON loader,
    parse thành Job objects, embed, trả về list[Job] với embedding đã set.
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

    # Gán embedding vào từng Job
    for job, emb in zip(job_objects, all_embeddings):
        job.embedding = emb

    logger.info(f"Embedded {len(job_objects)} jobs successfully")
    return job_objects


async def close_client():
    """Gọi khi shutdown app để đóng httpx client."""
    global _client_instance
    if _client_instance:
        await _client_instance.aclose()
        _client_instance = None
