# Keyword derivation and tokenisation for hybrid search.
from __future__ import annotations

import re
from typing import Any, Iterable


_MAX_KEYWORDS = 24

_TOKEN_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9+.#\-/]*")

_REWRITE_TOKEN_RE = re.compile(r"[\w.+#\-]+")

_NON_ASCII_RE = re.compile(r"[^\x00-\x7F]")


def _latin_tokens_or_phrase(text: str) -> list[str]:
    """Tokenise a phrase with a Vietnamese / CJK-friendly fallback.

    * Pure ASCII (e.g. "Senior Backend") → list of tokens.
    * Mixed (e.g. "Lập trình viên Python") → ``[phrase, *latin_tokens]`` so both the full phrase and the technical tokens can match.
    * Pure non-ASCII (e.g. "Lập trình viên") → ``[phrase]`` (single unit).
    """
    phrase = (text or "").strip()
    if not phrase:
        return []
    latin_tokens: list[str] = []
    for word in phrase.split():
        if _NON_ASCII_RE.search(word):
            continue
        latin_tokens.extend(_TOKEN_RE.findall(word))
    if not latin_tokens:
        return [phrase]
    if _NON_ASCII_RE.search(phrase):
        return [phrase, *latin_tokens]
    return latin_tokens


def _dedup_preserve_order(items: Iterable[str]) -> list[str]:
    """Case-insensitive dedup that preserves first-seen casing and order."""
    seen: set[str] = set()
    out: list[str] = []
    for item in items:
        key = item.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(item)
    return out


def _expand_phrase(phrase: str) -> list[str]:
    """Tokenise a phrase, but keep single tokens verbatim to preserve casing like "C++" / "Node.js"."""
    text = str(phrase or "").strip()
    if not text:
        return []
    return _latin_tokens_or_phrase(text) if " " in text else [text]


def _extend_from_list_slot(parsed: dict[str, Any], key: str, parts: list[str]) -> None:
    values = parsed.get(key)
    if not isinstance(values, list):
        return
    for raw in values:
        parts.extend(_expand_phrase(str(raw)))


def derive_search_keywords(parsed: dict[str, Any]) -> list[str]:
    """Combines (in order):
      1. Existing ``parsed["keywords"]`` (highest priority for ordering).
      2. Tokens from ``job_level``.
      3. ``skills``, ``job_domains``, ``must_include_keywords``.

    Always deduped (case-insensitive) and capped at ``_MAX_KEYWORDS``.
    """
    parts: list[str] = []

    existing = parsed.get("keywords")
    if isinstance(existing, list):
        parts.extend(s for s in (str(x).strip() for x in existing) if s)

    job_level = str(parsed.get("job_level") or "").strip()
    if job_level:
        parts.extend(_latin_tokens_or_phrase(job_level))

    _extend_from_list_slot(parsed, "skills", parts)
    _extend_from_list_slot(parsed, "job_domains", parts)
    _extend_from_list_slot(parsed, "must_include_keywords", parts)

    return _dedup_preserve_order(parts)[:_MAX_KEYWORDS]


def enrich_parsed_query_for_retrieval(parsed: dict[str, Any]) -> dict[str, Any]:
    """Return a shallow copy of ``parsed`` with ``keywords`` filled when derivable."""
    out = dict(parsed)
    keywords = derive_search_keywords(out)
    if keywords:
        out["keywords"] = keywords
    return out


def keywords_from_rewritten(rewritten: str) -> list[str]:
    """Tokenise a rewritten query: Drops 1-char noise, dedups case-insensitively, caps at ``_MAX_KEYWORDS``.
    """
    if not rewritten:
        return []
    seen: set[str] = set()
    out: list[str] = []
    for tok in _REWRITE_TOKEN_RE.findall(rewritten):
        if len(tok) <= 1:
            continue
        key = tok.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(tok)
        if len(out) >= _MAX_KEYWORDS:
            break
    return out
