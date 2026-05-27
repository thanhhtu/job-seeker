from __future__ import annotations

import json

from langchain_core.messages import AIMessage

from src.agent.states.state import JobSearchState
from src.core.logger import get_logger
from src.models.job_schema import Job

logger = get_logger(__name__)


def _job_to_card(job: Job) -> dict:
    card: dict = {
        "title": job.title,
        "company_name": job.company_name,
    }
    if job.locations:
        card["locations"] = list(job.locations)
    if job.salary_min or job.salary_max:
        card["salary_min"] = job.salary_min
        card["salary_max"] = job.salary_max
        card["salary_currency"] = job.salary_currency
    if job.salary_negotiable:
        card["salary_negotiable"] = True
    if job.work_mode and job.work_mode != "unknown":
        card["work_mode"] = job.work_mode
    if job.job_level:
        card["job_level"] = job.job_level
    if job.skills:
        card["skills"] = list(job.skills[:10])
    if job.experience_years_min:
        card["experience_years_min"] = job.experience_years_min
    if job.url:
        card["url"] = job.url
    return card


def output_node(state: JobSearchState) -> dict:
    """Build structured JSON output and a text summary for conversation history."""
    reranked_results: list[Job] = state.get("reranked_results") or []
    gen = state.get("generation_result") or {}
    referenced_jobs: list[Job] = gen.get("referenced_jobs") or []
    match_summary: str = (gen.get("match_summary") or "").strip()
    recommendations: list[dict] = gen.get("recommendations") or []
    suggested_actions: list[str] = gen.get("suggested_actions") or []
    clarification_prompt: str = (state.get("clarification_prompt") or "").strip()

    if clarification_prompt:
        structured = {
            "type": "clarification",
            "message": clarification_prompt,
        }
        text_for_history = clarification_prompt

    elif not reranked_results or not referenced_jobs:
        no_result_msg = (
            "Không tìm thấy công việc phù hợp với tiêu chí của bạn. "
            "Hãy thử điều chỉnh từ khóa hoặc nới lỏng bộ lọc."
        )
        if match_summary:
            no_result_msg = match_summary
        structured = {
            "type": "no_results",
            "message": no_result_msg,
        }
        text_for_history = no_result_msg

    else:
        structured = {
            "type": "jobs",
            "match_summary": match_summary,
            "recommendations": recommendations,
            "suggested_actions": suggested_actions,
            "jobs": [_job_to_card(job) for job in referenced_jobs],
        }
        text_for_history = match_summary or f"Tìm thấy {len(referenced_jobs)} công việc phù hợp."

    logger.info("output_node: type=%s, %d jobs", structured["type"], len(referenced_jobs))

    output_json = json.dumps(structured, ensure_ascii=False)

    result: dict = {
        "output": output_json,
        "messages": [AIMessage(content=text_for_history)],
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
                "generation_result": {},
            }
        )
    return result
