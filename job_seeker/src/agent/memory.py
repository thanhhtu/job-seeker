from __future__ import annotations

import re
from typing import Any

# LLM / client marks explicit removal of a slot (null is treated as "omit", not "clear")
CLEAR_SLOT_SENTINEL = "__CLEAR__"


def _is_empty_value(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str) and not value.strip():
        return True
    if isinstance(value, (list, dict)) and len(value) == 0:
        return True
    return False


def _is_clear_sentinel(value: Any) -> bool:
    return isinstance(value, str) and value.strip() == CLEAR_SLOT_SENTINEL


def merge_slot_memory(old: dict[str, Any], new: dict[str, Any]) -> dict[str, Any]:
    """Merge slot dict from the latest turn into persisted slots.

    - ``__CLEAR__`` removes that key (user explicitly dropped the constraint).
    - Never overwrite an existing concrete value with null / empty string / empty list.
    - Non-empty new values win (recency).
    """
    merged: dict[str, Any] = dict(old)
    for key, value in new.items():
        if _is_clear_sentinel(value):
            merged.pop(key, None)
            continue
        if _is_empty_value(value):
            continue
        merged[key] = value
    return merged


_TOKEN_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9+.#\-]{1,}")


def _tokens_from_text(text: str) -> list[str]:
    return _TOKEN_RE.findall(text or "")


def _latin_tokens_or_phrase(text: str) -> list[str]:
    """Prefer Latin tokens; if none (e.g. Vietnamese-only), keep the phrase as one unit."""
    tokens = _tokens_from_text(text)
    if tokens:
        return tokens
    t = (text or "").strip()
    return [t] if t else []


def derive_search_keywords(parsed: dict[str, Any]) -> list[str]:
    """Build BM25 / vector keyword list from role + skills if not already set."""
    existing = parsed.get("keywords")
    if isinstance(existing, list):
        cleaned = [str(x).strip() for x in existing if str(x).strip()]
        if cleaned:
            return cleaned

    parts: list[str] = []
    role = str(parsed.get("role") or "").strip()
    if role:
        parts.extend(_latin_tokens_or_phrase(role))
    skills = parsed.get("skills") or []
    if isinstance(skills, list):
        for s in skills:
            s = str(s).strip()
            if s:
                parts.extend(_latin_tokens_or_phrase(s) if " " in s else [s])

    seen: set[str] = set()
    out: list[str] = []
    for p in parts:
        low = p.lower()
        if low not in seen:
            seen.add(low)
            out.append(p)
    return out[:24]


def enrich_parsed_query_for_retrieval(parsed: dict[str, Any]) -> dict[str, Any]:
    out = dict(parsed)
    kws = derive_search_keywords(out)
    if kws:
        out["keywords"] = kws
    return out


def _has_nonempty_keywords(p: dict[str, Any]) -> bool:
    kws = p.get("keywords", [])
    return isinstance(kws, list) and any(str(k).strip() for k in kws)


def _has_nonempty_skills(p: dict[str, Any]) -> bool:
    skills = p.get("skills") or []
    return isinstance(skills, list) and any(str(s).strip() for s in skills)


def _has_location_with_filter(p: dict[str, Any]) -> bool:
    if not str(p.get("location", "")).strip():
        return False
    return any(
        [
            str(p.get("job_level", "")).strip(),
            str(p.get("work_mode", "")).strip(),
            p.get("experience_years") is not None,
            p.get("salary_min") is not None,
        ]
    )


def ready_for_retrieval(parsed: dict[str, Any]) -> bool:
    """Single definition of 'enough structured signal to run hybrid search' (no raw_query)."""
    p = enrich_parsed_query_for_retrieval(dict(parsed))
    if _has_nonempty_keywords(p):
        return True
    if str(p.get("role", "")).strip():
        return True
    if _has_nonempty_skills(p):
        return True
    if _has_location_with_filter(p):
        return True
    return False


def compute_missing_slots(parsed: dict[str, Any]) -> list[str]:
    """Canonical missing-slot keys for routing + clarification (same truth as ready_for_retrieval)."""
    p = enrich_parsed_query_for_retrieval(dict(parsed))
    if ready_for_retrieval(p):
        return []
    loc = str(p.get("location", "")).strip()
    if loc and not _has_location_with_filter(p):
        return ["location_needs_role_or_filters"]
    return ["search_context"]


def keywords_from_rewritten(rewritten: str) -> list[str]:
    """Tokenise rewritten_query for BM25 tsquery (ASCII word chars + common tech punctuation)."""
    return [w for w in re.findall(r"[\w\.\+\#\-]+", rewritten or "") if len(w) > 1][:24]
