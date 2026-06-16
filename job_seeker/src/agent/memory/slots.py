# Slot memory: merge logic, readiness checks, and missing-slot computation.
from __future__ import annotations

from typing import Any

from src.agent.constants import MissingSlot
from src.agent.memory.keywords import enrich_parsed_query_for_retrieval
from src.retrieval._filters import normalize_work_modes


CLEAR_SLOT_SENTINEL = "__CLEAR__"


# Value checks 
def _is_empty_value(value: Any) -> bool:
    """True for None, whitespace-only strings, and empty collections."""
    if value is None:
        return True
    if isinstance(value, str):
        return not value.strip()
    if isinstance(value, (list, tuple, set, dict)):
        return len(value) == 0
    return False


def _is_clear_sentinel(value: Any) -> bool:
    return isinstance(value, str) and value.strip() == CLEAR_SLOT_SENTINEL


# Merge
def merge_slot_memory(old: dict[str, Any], new: dict[str, Any]) -> dict[str, Any]:
    """Merge per-turn slot updates into stored slot memory.

    Rules:
      * ``"__CLEAR__"``        → drop the key (user dropped that constraint).
      * Empty (None / "" / []) → keep the old value (treated as "not mentioned").
      * Anything else          → overwrite.
    """
    merged: dict[str, Any] = dict(old)
    for key, value in new.items():
        if _is_clear_sentinel(value):
            merged.pop(key, None)
        elif not _is_empty_value(value):
            merged[key] = value
    return merged


# Readiness / missing slots
def _has_nonempty_str_list(p: dict[str, Any], key: str) -> bool:
    value = p.get(key)
    return isinstance(value, list) and any(str(item).strip() for item in value)


def _has_location_with_filter(p: dict[str, Any]) -> bool:
    """Location alone is too vague; require pairing with another constraint."""
    if not str(p.get("location") or "").strip():
        return False
    return (
        bool(str(p.get("job_level") or "").strip())
        or bool(normalize_work_modes(p.get("work_mode")))
        or p.get("candidate_experience_years") is not None
        or p.get("job_experience_min") is not None
        or p.get("job_experience_max") is not None
        or p.get("salary_min") is not None
        or p.get("salary_max") is not None
    )


def _ready_for_retrieval_enriched(p: dict[str, Any]) -> bool:
    """Same as ``ready_for_retrieval`` but assumes ``p`` is already enriched."""
    if _has_nonempty_str_list(p, "keywords"):
        return True
    if str(p.get("job_level") or "").strip():
        return True
    if _has_nonempty_str_list(p, "skills"):
        return True
    if _has_nonempty_str_list(p, "job_domains"):
        return True
    if _has_nonempty_str_list(p, "must_include_keywords"):
        return True
    return _has_location_with_filter(p)


def ready_for_retrieval(parsed: dict[str, Any]) -> bool:
    """True when ``parsed`` carries enough structured signal to run hybrid search."""
    return _ready_for_retrieval_enriched(enrich_parsed_query_for_retrieval(parsed))


def compute_missing_slots(parsed: dict[str, Any]) -> list[str]:
    """Canonical missing-slot keys for routing + clarification.

    Mirrors ``ready_for_retrieval`` so the router and the clarifier agree on "is there enough signal?".
    """
    p = enrich_parsed_query_for_retrieval(parsed)

    if _ready_for_retrieval_enriched(p):
        return []

    has_salary_bound = p.get("salary_min") is not None or p.get("salary_max") is not None
    has_salary_currency = bool(str(p.get("salary_currency") or "").strip())
    if has_salary_bound and not has_salary_currency:
        return [MissingSlot.SALARY_CURRENCY]

    location = str(p.get("location") or "").strip()
    if location and not _has_location_with_filter(p):
        return [MissingSlot.LOCATION_NEEDS_ROLE_OR_FILTERS]
    return [MissingSlot.SEARCH_CONTEXT]
