from __future__ import annotations

import asyncio
import random
from typing import Any

from langchain_core.messages import HumanMessage, SystemMessage
from langchain_mistralai import ChatMistralAI
from pydantic import BaseModel, Field

from src.agent.llm.retry import ainvoke_with_retry
from src.agent.states.state import JobSearchState
from src.core.config import settings
from src.core.logger import get_logger
from src.models.job_schema import Job

logger = get_logger(__name__)

_llm: ChatMistralAI | None = None
_structured_llm: Any | None = None

# Context limits
_MAX_JOBS      = 5     # only top-N jobs sent to LLM
_MAX_SKILLS    = 8     # skills per job

_GENERATION_SYSTEM_PROMPT = """\
<role>
You are a job recommendation assistant.
</role>

<task>
Create a grounded recommendation from provided job context only.
</task>

<rules>
- Use only facts available in CONTEXT jobs.
- If information is missing or uncertain, state that clearly.
- Produce user-facing Vietnamese for match_summary, recommendation reasons, and suggested_actions.
- Prioritize relevance to the latest user intent (summary + canonical query + latest message).
- Keep each reason specific and non-generic.
</rules>
"""


# Structured output schema
class JobRecommendation(BaseModel):
    rank: int = Field(description="Position of the job in the context list (1-based)")
    title: str = Field(description="Job title")
    company: str = Field(description="Company name")
    reason: str = Field(description="Brief reason why this job is recommended (in Vietnamese)")


class GenerateResult(BaseModel):
    match_summary: str = Field(
        description="Short summary of how well the search results match the user's request (in Vietnamese)",
    )
    recommendations: list[JobRecommendation] = Field(
        description="Top 3-5 recommended jobs with a specific reason for each (in Vietnamese)",
    )
    suggested_actions: list[str] = Field(
        description="2-3 next actions the user can take, e.g. refine filters, view details, apply (in Vietnamese)",
    )


# LLM singletons
def _get_llm() -> ChatMistralAI:
    global _llm
    if _llm is None:
        _llm = ChatMistralAI(
            model="mistral-large-latest",
            api_key=settings.mistral_api_key,
            temperature=0.2,
        )
    return _llm


def _get_structured_llm():
    global _structured_llm
    if _structured_llm is None:
        _structured_llm = _get_llm().with_structured_output(GenerateResult)
    return _structured_llm


# Helpers
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
        currency = job.salary_currency or "UNKNOWN"
        lo = f"{job.salary_min:,.0f}" if job.salary_min else "?"
        hi = f"{job.salary_max:,.0f}" if job.salary_max else "?"
        lines.append(f"Salary: {lo}-{hi} {currency}/month")
    if job.url:
        lines.append(f"URL: {job.url}")
    return "\n".join(lines)


async def _invoke_structured_with_retry(messages: list) -> GenerateResult:
    """Call the structured-output LLM with shared retry policy."""
    return await ainvoke_with_retry(
        lambda: _get_structured_llm().ainvoke(messages),
        logger=logger,
        operation_name="generate_node",
    )


def _empty_result(answer: str) -> dict:
    return {
        "generation_result": {
            "match_summary": answer,
            "recommendations": [],
            "suggested_actions": [],
            "referenced_jobs": [],
        },
    }


# Node
async def generate_node(state: JobSearchState) -> dict:
    raw_query: str = state.get("raw_query", "").strip()
    summary: str = (state.get("conversation_summary") or "").strip()
    rewritten: str = (state.get("rewritten_query") or "").strip()
    reranked: list[Job] = state.get("reranked_results", [])

    if not reranked:
        return _empty_result(
            "Không tìm thấy công việc phù hợp với yêu cầu của bạn. "
            "Hãy thử mở rộng từ khóa, địa điểm hoặc yêu cầu kinh nghiệm."
        )

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
            _GENERATION_SYSTEM_PROMPT
        )),
        HumanMessage(content=(
            f"{user_preamble}\n\n"
            f"CONTEXT jobs:\n{context}\n\n"
            "<output_requirements>\n"
            "- match_summary: Brief assessment of fit quality and notable trade-offs\n"
            "- recommendations: 3-5 jobs ranked by relevance (rank, title, company, reason)\n"
            "- suggested_actions: 2-3 concrete next steps for the user\n"
            "</output_requirements>"
        )),
    ]

    result: GenerateResult = await _invoke_structured_with_retry(messages)

    recommendations = [
        {"rank": i, "title": r.title, "company": r.company, "reason": r.reason}
        for i, r in enumerate(
            sorted(result.recommendations, key=lambda r: r.rank), start=1
        )
    ]

    logger.info(
        "generate_node: structured answer produced "
        "(summary=%d chars, %d recommendations, %d actions)",
        len(result.match_summary),
        len(recommendations),
        len(result.suggested_actions),
    )

    return {
        "generation_result": {
            "match_summary": result.match_summary,
            "recommendations": recommendations,
            "suggested_actions": result.suggested_actions,
            "referenced_jobs": jobs_to_use,
        },
    }
