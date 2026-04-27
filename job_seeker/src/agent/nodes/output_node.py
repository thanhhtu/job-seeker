from __future__ import annotations

from src.agent.state import JobSearchState
from src.core.logger import get_logger
from src.models.job_schema import Job

logger = get_logger(__name__)


def _format_job(rank: int, job: Job) -> str:
    lines: list[str] = []

    lines.append(f"{'─' * 60}")
    lines.append(f"#{rank}  {job.title}")
    lines.append(f"{job.company_name}")

    if job.locations:
        lines.append(f"    {', '.join(job.locations)}")

    if job.salary_min or job.salary_max:
        currency = job.salary_currency or "USD"
        lo = f"{job.salary_min:,.0f}" if job.salary_min else "?"
        hi = f"{job.salary_max:,.0f}" if job.salary_max else "?"
        lines.append(f"    {lo} – {hi} {currency}/month")
    elif job.salary_negotiable:
        lines.append("    Negotiable")

    # Work mode & level
    meta_parts: list[str] = []
    if job.work_mode and job.work_mode != "unknown":
        meta_parts.append(job.work_mode.capitalize())
    if job.job_level:
        meta_parts.append(job.job_level.capitalize())
    if job.experience_years_min:
        meta_parts.append(f"{job.experience_years_min}+ years experience")
    if meta_parts:
        lines.append(f"    {' · '.join(meta_parts)}")

    # Skills (max 8)
    if job.skills:
        skill_str = ", ".join(job.skills[:8])
        if len(job.skills) > 8:
            skill_str += f" +{len(job.skills) - 8} more"
        lines.append(f"    {skill_str}")

    # Link
    if job.url:
        lines.append(f"    {job.url}")

    return "\n".join(lines)


def output_node(state: JobSearchState) -> dict:
    reranked_results: list[Job] = state.get("reranked_results", [])
    generated_answer: str = state.get("generated_answer", "").strip()

    if not reranked_results:
        output = (
            "No jobs found matching your criteria.\n"
            "Try adjusting your keywords or relaxing the filters."
        )
        if generated_answer:
            output = f"{generated_answer}\n\n{output}"
        return {"output": output}

    header = (
        f"Found {len(reranked_results)} most relevant jobs:\n"
        f"(Results ranked by BGE Reranker v2-m3)\n"
    )

    job_blocks = "\n\n".join(
        _format_job(rank, job)
        for rank, job in enumerate(reranked_results, start=1)
    )

    sections: list[str] = []
    if generated_answer:
        sections.append("LLM recommendation:\n" + generated_answer)

    sections.append(header + "\n" + job_blocks + "\n" + "─" * 60)
    output = "\n\n".join(sections)

    logger.info(f"Output node: formatted {len(reranked_results)} jobs")
    return {"output": output}
