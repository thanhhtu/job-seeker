# src/agent/nodes/needs_input_node.py
from __future__ import annotations

from src.agent.state import JobSearchState

# ── Response messages ──────────────────────────────────────────────────────────

_MSG_EMPTY_QUERY = (
    "Please provide a job title, skills, or location to search."
)

_MSG_SEARCH_CONTEXT = (
    "I need a clearer job target to search: please share a role or job title, main skills (e.g. Python, React), or specific keywords."
)

_MSG_LOCATION_NEEDS_MORE = (
    "You mentioned a location. To search effectively, add either a target role/skills or filters such as seniority, work mode (remote/onsite), experience, or salary."
)

_MSG_VAGUE_QUERY = (
    "Your message is still very general. Please name a role, skills, or location you care about."
)


def _normalize(text: str) -> str:
    return " ".join(text.lower().split())


_VAGUE_EXACT: frozenset[str] = frozenset(
    {
        "job",
        "jobs",
        "work",
        "career",
        "find job",
        "find jobs",
        "job search",
        "viec",
        "viec lam",
        "tim viec",
        "tim viec lam",
        "xin viec",
    }
)


def _is_vague_exact(query: str) -> bool:
    return _normalize(query) in _VAGUE_EXACT


def _clarify_for_missing_slots(missing: list[str], raw_query: str) -> str:
    if "location_needs_role_or_filters" in missing:
        return _MSG_LOCATION_NEEDS_MORE
    if "search_context" in missing:
        if _is_vague_exact(raw_query):
            return _MSG_VAGUE_QUERY
        return _MSG_SEARCH_CONTEXT
    return _MSG_SEARCH_CONTEXT


def needs_input_node(state: JobSearchState) -> dict:
    raw_query = (state.get("raw_query") or "").strip()
    missing = state.get("missing_slots") or []

    if not raw_query:
        message = _MSG_EMPTY_QUERY
    elif missing:
        message = _clarify_for_missing_slots(missing, raw_query)
    else:
        message = _MSG_SEARCH_CONTEXT

    return {
        "output": message,
        "clarification_prompt": message,
    }
