# src/agent/nodes/generate_node.py
from __future__ import annotations

import asyncio
import random

import httpx
from langchain_core.messages import HumanMessage, SystemMessage
from langchain_mistralai import ChatMistralAI

from src.agent.state import JobSearchState
from src.core.config import settings
from src.core.logger import get_logger
from src.models.job_schema import Job

logger = get_logger(__name__)

_llm: ChatMistralAI | None = None

# ── Retry config ───────────────────────────────────────────────────────────────
_MAX_RETRIES   = 4
_BASE_DELAY    = 2.0   # seconds
_MAX_DELAY     = 30.0
_JITTER        = 0.5   # ± seconds added to each delay

# ── Context limits ─────────────────────────────────────────────────────────────
_MAX_JOBS      = 5     # only top-N jobs sent to LLM
_MAX_SKILLS    = 8     # skills per job


def _get_llm() -> ChatMistralAI:
    global _llm
    if _llm is None:
        _llm = ChatMistralAI(
            model="mistral-large-latest",
            api_key=settings.mistral_api_key,
            temperature=0.2,
        )
    return _llm


def _job_context(rank: int, job: Job) -> str:
    lines = [
        f"Rank: {rank}",
        f"Title: {job.title}",
        f"Company: {job.company_name}",
    ]
    if job.locations:
        lines.append(f"Location: {', '.join(job.locations)}")
    if job.work_mode and job.work_mode != "unknown":
        lines.append(f"Work mode: {job.work_mode}")
    if job.job_level:
        lines.append(f"Level: {job.job_level}")
    if job.skills:
        lines.append(f"Skills: {', '.join(job.skills[:_MAX_SKILLS])}")
    if job.salary_min or job.salary_max:
        currency = job.salary_currency or "USD"
        lo = f"{job.salary_min:,.0f}" if job.salary_min else "?"
        hi = f"{job.salary_max:,.0f}" if job.salary_max else "?"
        lines.append(f"Salary: {lo}-{hi} {currency}/month")
    if job.url:
        lines.append(f"URL: {job.url}")
    return "\n".join(lines)


async def _invoke_with_retry(messages: list) -> str:
    """Call the LLM with exponential back-off on 429 responses."""
    delay = _BASE_DELAY
    for attempt in range(1, _MAX_RETRIES + 1):
        try:
            response = await _get_llm().ainvoke(messages)
            return str(response.content).strip()

        except httpx.HTTPStatusError as exc:
            if exc.response.status_code != 429 or attempt == _MAX_RETRIES:
                raise
            jitter   = random.uniform(-_JITTER, _JITTER)
            wait     = min(delay + jitter, _MAX_DELAY)
            logger.warning(
                "generate_node: rate-limited (attempt %d/%d). "
                "Retrying in %.1fs …",
                attempt, _MAX_RETRIES, wait,
            )
            await asyncio.sleep(wait)
            delay *= 2   # exponential back-off

    raise RuntimeError("generate_node: exhausted retries")  # unreachable


async def generate_node(state: JobSearchState) -> dict:
    raw_query: str = state.get("raw_query", "").strip()
    summary: str = (state.get("conversation_summary") or "").strip()
    rewritten: str = (state.get("rewritten_query") or "").strip()
    reranked: list[Job] = state.get("reranked_results", [])

    if not reranked:
        return {
            "generated_answer": (
                "No matching jobs were found for your request. "
                "Try broadening keywords, location, or experience constraints."
            )
        }

    # Cap how many jobs we send to stay inside token/rate limits
    jobs_to_use = reranked[:_MAX_JOBS]
    context = "\n\n".join(
        _job_context(rank, job)
        for rank, job in enumerate(jobs_to_use, start=1)
    )

    header_parts = []
    if summary:
        header_parts.append(f"Conversation summary:\n{summary}")
    if rewritten:
        header_parts.append(f"Canonical search query:\n{rewritten}")
    header_parts.append(f"Latest user message:\n{raw_query or '(none)'}")
    user_preamble = "\n\n".join(header_parts)

    messages = [
        SystemMessage(content=(
            "You are a job search assistant. "
            "Answer only from the provided CONTEXT jobs. "
            "If information is missing, explicitly say so."
        )),
        HumanMessage(content=(
            f"{user_preamble}\n\n"
            f"CONTEXT jobs:\n{context}\n\n"
            "Write a concise recommendation:\n"
            "1) short fit summary\n"
            "2) top 3-5 suggested jobs with reasons\n"
            "3) next action for the user"
        )),
    ]

    generated_answer = await _invoke_with_retry(messages)
    logger.info("generate_node: answer produced (%d chars)", len(generated_answer))
    return {"generated_answer": generated_answer}
