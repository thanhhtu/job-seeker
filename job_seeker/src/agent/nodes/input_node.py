# src/agent/nodes/input_node.py
from __future__ import annotations

from langchain_core.messages import HumanMessage

from src.agent.state import JobSearchState


def _extract_text(content) -> str:
    """Normalize content: str hoặc list[dict] → str thuần."""
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


def input_node(state: JobSearchState) -> dict:
    """Extract raw_query from latest user turn (including empty content)."""
    messages = state.get("messages") or []

    for msg in reversed(messages):
        if isinstance(msg, HumanMessage):
            return {
                "raw_query": _extract_text(msg.content),
                "needs_input_prompt": "",
            }
        elif isinstance(msg, dict) and msg.get("role") == "user":
            return {
                "raw_query": _extract_text(msg.get("content") or ""),
                "needs_input_prompt": "",
            }

    # Fallback only when there is no user message in this state snapshot.
    existing = _extract_text(state.get("raw_query") or "")
    return {"raw_query": existing, "needs_input_prompt": ""}
