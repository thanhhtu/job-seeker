from __future__ import annotations

import asyncio
import random
from collections.abc import Awaitable, Callable
from typing import TypeVar

import httpx

T = TypeVar("T")


async def ainvoke_with_retry(
    invoke_fn: Callable[[], Awaitable[T]],
    *,
    logger,
    operation_name: str,
    max_retries: int = 4,
    base_delay: float = 2.0,
    max_delay: float = 30.0,
    jitter: float = 0.5,
    retry_status_codes: tuple[int, ...] = (429,),
) -> T:
    """Invoke an async LLM call with exponential backoff on transient HTTP errors."""
    delay = base_delay
    for attempt in range(1, max_retries + 1):
        try:
            return await invoke_fn()
        except httpx.HTTPStatusError as exc:
            status_code = exc.response.status_code
            should_retry = status_code in retry_status_codes and attempt < max_retries
            if not should_retry:
                raise

            wait = min(delay + random.uniform(-jitter, jitter), max_delay)
            logger.warning(
                "%s: rate-limited/status=%d (attempt %d/%d), retrying in %.1fs",
                operation_name,
                status_code,
                attempt,
                max_retries,
                wait,
            )
            await asyncio.sleep(wait)
            delay *= 2

    raise RuntimeError(f"{operation_name}: exhausted retries")
