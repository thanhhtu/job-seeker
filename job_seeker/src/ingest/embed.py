import asyncio
import threading
from os import getenv

from langchain_mistralai import MistralAIEmbeddings

from src.core.config import settings
from src.core.logger import get_logger
from src.models.job_schema import Job

logger = get_logger(__name__)

MISTRAL_MODEL = "mistral-embed"
EMBED_BATCH_SIZE = 64  # Mistral cho phép tối đa 2048 tokens/text, batch hợp lý

_embedder_instance: MistralAIEmbeddings | None = None
_embedder_initializing = False
_embedder_lock = threading.Lock()


def _create_embedder_sync(api_key: str) -> MistralAIEmbeddings:
    return MistralAIEmbeddings(
        mistral_api_key=api_key,
        model=MISTRAL_MODEL,
    )


async def get_embedder_async() -> MistralAIEmbeddings:
    global _embedder_instance, _embedder_initializing

    if _embedder_instance is not None:
        return _embedder_instance

    if _embedder_initializing:
        while _embedder_initializing:
            await asyncio.sleep(0.1)
        return _embedder_instance

    with _embedder_lock:
        if _embedder_instance is None:
            api_key = settings.mistral_api_key  # dùng settings thay vì getenv
            _embedder_initializing = True

    loop = asyncio.get_event_loop()
    embedder = await loop.run_in_executor(None, _create_embedder_sync, api_key)

    _embedder_instance = embedder
    _embedder_initializing = False
    return embedder


async def embed_query_async(embedder: MistralAIEmbeddings, query: str) -> list[float]:
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, embedder.embed_query, query)


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
    embedder = await get_embedder_async()
    loop = asyncio.get_event_loop()

    # Parse dict -> Job
    job_objects: list[Job] = [Job.from_json(j) for j in jobs]

    # Build texts to embed
    texts = [_build_embed_text(job) for job in job_objects]

    # Batch embed (chạy trong thread để không block event loop)
    all_embeddings: list[list[float]] = []
    for i in range(0, len(texts), EMBED_BATCH_SIZE):
        batch = texts[i : i + EMBED_BATCH_SIZE]
        logger.info(f"Embedding batch {i // EMBED_BATCH_SIZE + 1} ({len(batch)} jobs)...")
        embeddings = await loop.run_in_executor(
            None, embedder.embed_documents, batch
        )
        all_embeddings.extend(embeddings)

    # Gán embedding vào từng Job
    for job, emb in zip(job_objects, all_embeddings):
        job.embedding = emb

    logger.info(f"Embedded {len(job_objects)} jobs")
    return job_objects
