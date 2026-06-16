"""Shared httpx AsyncClient pools with keep-alive connection reuse.

Creating an httpx.AsyncClient per request opens a fresh TCP (and TLS) handshake
every time. These helpers return long-lived clients keyed by base_url so that
connections are pooled and kept alive across calls (Ollama, BGE reranker, etc.).
"""

from __future__ import annotations

import asyncio

import httpx

from src.core.logger import get_logger

logger = get_logger(__name__)

# Keep-alive connection pool tuning shared by all outbound HTTP clients.
DEFAULT_LIMITS = httpx.Limits(
    max_connections=100,
    max_keepalive_connections=20,
    keepalive_expiry=30.0,
)

_clients: dict[str, httpx.AsyncClient] = {}
_lock = asyncio.Lock()


async def get_client(
    base_url: str,
    *,
    timeout: float = 60.0,
) -> httpx.AsyncClient:
    """Return a shared keep-alive client for ``base_url``, creating it once."""
    client = _clients.get(base_url)
    if client is not None and not client.is_closed:
        return client

    async with _lock:
        client = _clients.get(base_url)
        if client is not None and not client.is_closed:
            return client

        client = httpx.AsyncClient(
            base_url=base_url,
            timeout=timeout,
            limits=DEFAULT_LIMITS,
            headers={"Connection": "keep-alive"},
        )
        _clients[base_url] = client
        logger.info("Created keep-alive HTTP client for %s", base_url)
        return client


async def close_all_clients() -> None:
    """Close every shared HTTP client. Call on app shutdown."""
    clients = list(_clients.values())
    _clients.clear()
    for client in clients:
        if not client.is_closed:
            await client.aclose()
    if clients:
        logger.info("Closed %d shared HTTP client(s)", len(clients))
