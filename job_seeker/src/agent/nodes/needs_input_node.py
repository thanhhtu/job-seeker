from __future__ import annotations

from src.agent.state import JobSearchState

# ── Response messages ──────────────────────────────────────────────────────────

_MSG_EMPTY_QUERY = (
    "Please provide a job title, skills, or location to search."
)
_MSG_NO_PARSED = (
    "Please add more details (role, skills, or location) to refine the search."
)
_MSG_VAGUE_QUERY = (
    "Your request is still too general. Please share more details like role, "
    "skills, location, experience level, or work mode."
)
_MSG_MISSING_KEYWORDS = (
    "I need at least a target role or skills to search accurately. "
    "Please add desired position, skills, or specific job keywords."
)
_MSG_FALLBACK = (
    "Please add more details so I can refine the search."
)

# ── Vague query detection ──────────────────────────────────────────────────────

# Exact normalized phrases that carry no search signal on their own.
# Substrings are intentionally excluded — "tim viec lam o ha noi" has location
# context and should fall through to _MSG_MISSING_KEYWORDS instead.
_VAGUE_EXACT: frozenset[str] = frozenset({
    "job", "jobs", "work", "career",
    "find job", "find jobs", "job search",
    "viec", "viec lam",
    "tim viec", "tim viec lam", "xin viec",
})


def _normalize(text: str) -> str:
    """Lowercase + collapse whitespace."""
    return " ".join(text.lower().split())


def _is_vague_exact(query: str) -> bool:
    return _normalize(query) in _VAGUE_EXACT


# ── Minimum-context check ──────────────────────────────────────────────────────

def _has_keywords(parsed: dict) -> bool:
    return any(
        str(k).strip()
        for k in parsed.get("keywords", [])
        if isinstance(parsed.get("keywords"), list)
    )


def _has_location_with_filter(parsed: dict) -> bool:
    """Location alone is insufficient; require at least one structured filter."""
    if not str(parsed.get("location", "")).strip():
        return False
    return any([
        str(parsed.get("job_level", "")).strip(),
        str(parsed.get("work_mode", "")).strip(),
        parsed.get("experience_years") is not None,
        parsed.get("salary_min") is not None,
    ])


def has_minimum_search_context(raw_query: str, parsed: dict) -> bool:
    """True when the query carries enough signal to run retrieval safely."""
    if not raw_query.strip():
        return False
    return _has_keywords(parsed) or _has_location_with_filter(parsed)


# ── Node ───────────────────────────────────────────────────────────────────────

def _choose_message(raw_query: str, parsed_query: dict) -> str:
    """Pure function: pick the most informative clarification prompt."""
    if not raw_query:
        return _MSG_EMPTY_QUERY

    if not parsed_query:
        return _MSG_NO_PARSED

    if has_minimum_search_context(raw_query, parsed_query):
        # Caller should not have routed here, but degrade gracefully.
        return _MSG_FALLBACK

    return _MSG_VAGUE_QUERY if _is_vague_exact(raw_query) else _MSG_MISSING_KEYWORDS


def needs_input_node(state: JobSearchState) -> dict:
    raw_query = (state.get("raw_query") or "").strip()
    parsed_query = state.get("parsed_query") or {}

    message = _choose_message(raw_query, parsed_query)
    return {"output": message, "needs_input_prompt": message}
	