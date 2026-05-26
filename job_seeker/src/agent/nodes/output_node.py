from __future__ import annotations

from langchain_core.messages import AIMessage

from src.agent.state import JobSearchState
from src.core.logger import get_logger
from src.models.job_schema import Job

logger = get_logger(__name__)


def _format_job(rank: int, job: Job) -> str:
    lines: list[str] = []
    lines.append(f"{'─' * 60}")
    lines.append(f"#{rank}  {job.title}")
    lines.append(f"    {job.company_name}")

    if job.locations:
        lines.append(f"    {', '.join(job.locations)}")

    if job.salary_min or job.salary_max:
        currency = job.salary_currency or "UNKNOWN"
        lo = f"{job.salary_min:,.0f}" if job.salary_min else "?"
        hi = f"{job.salary_max:,.0f}" if job.salary_max else "?"
        lines.append(f"    {lo} – {hi} {currency}/month")
    elif job.salary_negotiable:
        lines.append("    Negotiable")

    meta_parts: list[str] = []
    if job.work_mode and job.work_mode != "unknown":
        meta_parts.append(job.work_mode.capitalize())
    if job.job_level:
        meta_parts.append(job.job_level.capitalize())
    if job.experience_years_min:
        meta_parts.append(f"{job.experience_years_min}+ years experience")
    if meta_parts:
        lines.append(f"    {' · '.join(meta_parts)}")

    if job.skills:
        skill_str = ", ".join(job.skills[:8])
        if len(job.skills) > 8:
            skill_str += f" +{len(job.skills) - 8} more"
        lines.append(f"    {skill_str}")

    if job.url:
        lines.append(f"    {job.url}")

    return "\n".join(lines)


def output_node(state: JobSearchState) -> dict:
    """Tổng hợp output cuối và append AIMessage vào messages.

    KHÔNG dùng existing_output từ state vì checkpointer có thể giữ
    output cũ từ lượt trước, gây trả về kết quả sai.
    """
    reranked_results: list[Job] = state.get("reranked_results") or []
    generated_answer: str = (state.get("generated_answer") or "").strip()
    clarification_prompt: str = (state.get("clarification_prompt") or "").strip()
    
    if clarification_prompt:
        final_output = clarification_prompt
    elif not reranked_results:
        final_output = (
            "Không tìm thấy công việc phù hợp với tiêu chí của bạn.\n"
            "Hãy thử điều chỉnh từ khóa hoặc nới lỏng bộ lọc."
        )
        if generated_answer:
            final_output = f"{generated_answer}\n\n{final_output}"
    else:
        header = (
            f"Tìm thấy {len(reranked_results)} công việc phù hợp nhất:\n"
            f"(Kết quả được xếp hạng bởi BGE Reranker v2-m3)\n"
        )
        job_blocks = "\n\n".join(
            _format_job(rank, job)
            for rank, job in enumerate(reranked_results, start=1)
        )
        sections: list[str] = []
        if generated_answer:
            sections.append("Gợi ý từ LLM:\n" + generated_answer)
        sections.append(header + "\n" + job_blocks + "\n" + "─" * 60)
        final_output = "\n\n".join(sections)

    logger.info("Output node: formatted %d jobs", len(reranked_results))

    result = {
        "output": final_output,
        # Tích lũy vào messages để lượt hội thoại sau có context
        "messages": [AIMessage(content=final_output)],
    }
    if not clarification_prompt:
        result.update(
            {
                "conversation_summary": "",
                "parsed_query": {},
                "missing_slots": [],
                "clarification_prompt": "",
                "rewritten_query": "",
                "bm25_results": [],
                "vector_results": [],
                "rrf_results": [],
                "reranked_results": [],
                "generated_answer": "",
            }
        )
    return result
