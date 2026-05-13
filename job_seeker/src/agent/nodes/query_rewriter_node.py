from __future__ import annotations

from langchain_core.messages import HumanMessage, SystemMessage
from langchain_mistralai import ChatMistralAI

from src.agent.state import JobSearchState
from src.core.config import settings
from src.core.logger import get_logger

logger = get_logger(__name__)

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
You are a search query optimisation assistant for a job search engine.

You will receive a JSON object containing structured job search intent.
Your task: produce a single, concise English search query string (≤ 20 words)
that a retrieval system can use directly.

Rules:
- Prioritise role, keywords, location, and job_level.
- Do NOT include salary or experience_years in the query string.
- Return ONLY the query string — no JSON, no markdown, no explanation.

Example input:
{
    "role": "Backend Developer", 
    "keywords": ["Python", "FastAPI"], 
    "location": "Hanoi", 
    "job_level": "senior"
}

Example output:
senior Backend Developer Python FastAPI Hanoi
"""


async def query_rewriter_node(state: JobSearchState) -> dict:
    """
    Converts structured parsed_query → a flat rewritten_query string for retrieval.

    Reads:  state["raw_query"]    — fallback if parsed_query is empty
            state["parsed_query"] — structured slots from understand_node
            state["conversation_summary"] — dialogue context for the rewriter
    Writes: state["rewritten_query"]
    """
    parsed_query: dict = state.get("parsed_query") or {}
    raw_query: str = (state.get("raw_query") or "").strip()
    summary: str = (state.get("conversation_summary") or "").strip()

    if not parsed_query and not raw_query:
        logger.warning("query_rewriter_node: both parsed_query and raw_query are empty")
        return {"rewritten_query": ""}

    parts: list[str] = []
    if summary:
        parts.append("Conversation summary:\n" + summary)
    if parsed_query:
        parts.append("Structured intent (JSON-like):\n" + str(parsed_query))
    if raw_query:
        parts.append("Latest user message:\n" + raw_query)
    user_content = "\n\n".join(parts)

    response = await _get_llm().ainvoke([
        SystemMessage(content=_SYSTEM_PROMPT),
        HumanMessage(content=user_content),
    ])

    rewritten = response.content.strip()
    logger.info("query_rewriter_node rewritten_query: %r", rewritten)
    return {"rewritten_query": rewritten}
