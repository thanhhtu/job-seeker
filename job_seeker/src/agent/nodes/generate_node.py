from __future__ import annotations

from langchain_core.messages import HumanMessage, SystemMessage
from langchain_mistralai import ChatMistralAI

from src.agent.state import JobSearchState
from src.core.config import settings
from src.core.logger import get_logger
from src.models.job_schema import Job

logger = get_logger(__name__)

_llm: ChatMistralAI | None = None


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
        lines.append(f"Skills: {', '.join(job.skills[:12])}")
    if job.salary_min or job.salary_max:
        currency = job.salary_currency or "USD"
        lo = f"{job.salary_min:,.0f}" if job.salary_min else "?"
        hi = f"{job.salary_max:,.0f}" if job.salary_max else "?"
        lines.append(f"Salary: {lo}-{hi} {currency}/month")
    if job.url:
        lines.append(f"URL: {job.url}")

    return "\n".join(lines)


async def generate_node(state: JobSearchState) -> dict:
    raw_query = state.get("raw_query", "").strip()
    reranked_results: list[Job] = state.get("reranked_results", [])

    if not reranked_results:
        return {
            "generated_answer": (
                "No matching jobs were found for your request. "
                "Try broadening keywords, location, or experience constraints."
            )
        }

    context = "\n\n".join(
        _job_context(rank, job)
        for rank, job in enumerate(reranked_results, start=1)
    )

    messages = [
        SystemMessage(
            content=(
                "You are a job search assistant. "
                "Answer only from the provided CONTEXT jobs. "
                "If information is missing, explicitly say so."
            )
        ),
        HumanMessage(
            content=(
                f"User query:\n{raw_query}\n\n"
                f"CONTEXT jobs:\n{context}\n\n"
                "Write a concise recommendation:\n"
                "1) short fit summary\n"
                "2) top 3-5 suggested jobs with reasons\n"
                "3) next action for the user"
            )
        ),
    ]

    response = await _get_llm().ainvoke(messages)
    generated_answer = str(response.content).strip()

    logger.info("Generate node produced LLM answer")
    return {"generated_answer": generated_answer}
