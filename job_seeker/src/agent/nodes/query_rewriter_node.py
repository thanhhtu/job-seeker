# src/agent/nodes/query_rewriter_node.py
from __future__ import annotations

import json
import re

from langchain_core.messages import HumanMessage, SystemMessage
from langchain_mistralai import ChatMistralAI

from src.agent.state import JobSearchState
from src.core.config import settings
from src.core.logger import get_logger

logger = get_logger(__name__)

# Lazy init — tránh lỗi nếu settings chưa load khi module được import
_llm: ChatMistralAI | None = None


def _get_llm() -> ChatMistralAI:
    global _llm
    if _llm is None:
        _llm = ChatMistralAI(
            model="mistral-large-latest",
            api_key=settings.mistral_api_key,
            temperature=0,
        )
    return _llm


_SYSTEM_PROMPT = """
You are a job search query analysis assistant.
Your task is to convert a user's search query into structured JSON.

Return JSON with the following fields (omit any field if information is not available):
{
  "keywords": [],          // list of skill/job keywords (in English)
  "location": "",          // job location (e.g., "Hanoi", "Ho Chi Minh City")
  "job_level": "",         // "intern", "fresher", "junior", "mid", "senior", "lead", "manager"
  "work_mode": "",         // "remote", "onsite", "hybrid"
  "experience_years": 0,   // minimum years of experience (integer)
  "salary_min": 0          // desired minimum salary (USD/month, float)
}

Return only pure JSON, no markdown, no additional explanation.
"""


def _extract_json(text: str) -> dict:
    try:
        return json.loads(text.strip())
    except json.JSONDecodeError:
        pass

    match = re.search(r"\{.*\}", text, re.DOTALL)
    if match:
        try:
            return json.loads(match.group())
        except json.JSONDecodeError:
            pass

    logger.warning("Could not parse LLM response as JSON: %r", text)
    return {}


async def query_rewriter_node(state: JobSearchState) -> dict:
    raw_query = state.get("raw_query", "").strip()
    if not raw_query:
        logger.warning("query_rewriter_node: empty raw_query")
        return {"parsed_query": {}}

    logger.info("Rewriting query: %r", raw_query)

    response = await _get_llm().ainvoke([
        SystemMessage(content=_SYSTEM_PROMPT),
        HumanMessage(content=raw_query),
    ])
    parsed_query = _extract_json(response.content)

    logger.info("Parsed query: %s", parsed_query)
    return {"parsed_query": parsed_query}
