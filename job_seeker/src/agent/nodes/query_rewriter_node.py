from __future__ import annotations

import json
import re

from langchain_mistralai import ChatMistralAI
from langchain_core.messages import HumanMessage, SystemMessage

from src.agent.state import JobSearchState
from src.core.config import settings
from src.core.logger import get_logger

logger = get_logger(__name__)

_llm = ChatMistralAI(
    model="mistral-large-latest",
    api_key=settings.mistral_api_key,
    temperature=0,
)

_SYSTEM_PROMPT = """
    You are a job search query analysis assistant.
    Your task is to convert a user's search query into structured JSON.

    Return JSON with the following fields (omit any field if information is not available):
    {
    "keywords": [],          // list of skill/job keywords (in English)
    "location": "",          // job location (string, e.g., "Hanoi", "Ho Chi Minh City")
    "job_level": "",         // level: "intern", "fresher", "junior", "mid", "senior", "lead", "manager"
    "work_mode": "",         // type: "remote", "onsite", "hybrid"
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

    logger.warning(f"Could not parse LLM response as JSON: {text!r}")
    return {}


async def query_rewriter_node(state: JobSearchState) -> dict:
    raw_query = state["raw_query"]
    logger.info(f"Rewriting query: {raw_query!r}")

    messages = [
        SystemMessage(content=_SYSTEM_PROMPT),
        HumanMessage(content=raw_query),
    ]

    response = await _llm.ainvoke(messages)
    parsed_query = _extract_json(response.content)

    logger.info(f"Parsed query: {parsed_query}")
    return {"parsed_query": parsed_query}
