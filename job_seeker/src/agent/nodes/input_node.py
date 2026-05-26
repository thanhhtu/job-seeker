from __future__ import annotations

from typing import Any

from langchain_core.messages import HumanMessage

from src.agent.state import JobSearchState
from src.core.logger import get_logger

logger = get_logger(__name__)


def _extract_text(content: Any) -> str:
    if isinstance(content, str):
        return content.strip()
    if isinstance(content, list):
        parts = []
        for block in content:
            if isinstance(block, str):
                parts.append(block)
            elif isinstance(block, dict) and block.get("type") == "text":
                parts.append(block.get("text", ""))
        return " ".join(parts).strip()
    return ""


def _new_turn_state(raw_query: str) -> dict:
    return {
        "raw_query": raw_query,
        "missing_slots": [],
        "clarification_prompt": "",
        "output": "",
        "rewritten_query": "",
        "bm25_results": [],
        "vector_results": [],
        "rrf_results": [],
        "reranked_results": [],
        "generated_answer": "",
    }


def input_node(state: JobSearchState) -> dict:
    messages = state.get("messages") or []

    for msg in reversed(messages):
        if isinstance(msg, HumanMessage):
            raw = _extract_text(msg.content)
            logger.info("input_node: source=HumanMessage raw_query_len=%d", len(raw))
            return _new_turn_state(raw)
            
        if isinstance(msg, dict) and msg.get("role") == "user":
            raw = _extract_text(msg.get("content") or "")
            logger.info("input_node: source=dict_user raw_query_len=%d", len(raw))
            return _new_turn_state(raw)

    raw = _extract_text(state.get("raw_query") or "")
    logger.warning(
        "input_node: no human message in state; falling back to raw_query (len=%d)",
        len(raw),
    )
    return _new_turn_state(raw)
