# src/agent/nodes/input_node.py
from __future__ import annotations

from langchain_core.messages import HumanMessage

from src.agent.state import JobSearchState

def _extract_text(content) -> str:
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
            return _new_turn_state(_extract_text(msg.content))
        if isinstance(msg, dict) and msg.get("role") == "user":
            return _new_turn_state(_extract_text(msg.get("content") or ""))

    return _new_turn_state(_extract_text(state.get("raw_query") or ""))
