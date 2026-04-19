import asyncio
import threading
from os import getenv

from langchain_mistralai import MistralAIEmbeddings

MISTRAL_MODEL = "mistral-embed"

_embedder_instance: MistralAIEmbeddings | None = None
_embedder_initializing = False
_embedder_lock = threading.Lock()


def _create_embedder_sync(api_key: str) -> MistralAIEmbeddings:
    """Create embedder synchronously (call from thread)."""
    return MistralAIEmbeddings(
        mistral_api_key=api_key,
        model=MISTRAL_MODEL,
    )


async def get_embedder_async() -> MistralAIEmbeddings:
    """Get the embedder instance, initializing in a thread if needed.

    This is the preferred way to get the embedder as it never blocks the event loop.
    """
    global _embedder_instance, _embedder_initializing

    if _embedder_instance is not None:
        return _embedder_instance

    # Check if another thread is initializing
    if _embedder_initializing:
        # Wait and retry until ready
        while _embedder_initializing:
            await asyncio.sleep(0.1)
        return _embedder_instance

    with _embedder_lock:
        if _embedder_instance is None:
            api_key = getenv("MISTRAL_API_KEY")
            if not api_key:
                raise ValueError("MISTRAL_API_KEY environment variable is not set")
            _embedder_initializing = True

    # Run initialization in thread
    loop = asyncio.get_event_loop()
    embedder = await loop.run_in_executor(
        None,
        _create_embedder_sync,
        api_key,
    )

    _embedder_instance = embedder
    _embedder_initializing = False
    return embedder


async def embed_query_async(embedder: MistralAIEmbeddings, query: str) -> list[float]:
    """Run embed_query in a thread to avoid blocking the event loop."""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, embedder.embed_query, query)
