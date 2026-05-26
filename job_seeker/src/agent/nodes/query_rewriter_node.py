from __future__ import annotations

import json

from langchain_core.messages import HumanMessage, SystemMessage
from langchain_mistralai import ChatMistralAI

from src.agent.memory.keywords import derive_search_keywords
from src.agent.state import JobSearchState
from src.core.config import settings
from src.core.logger import get_logger

logger = get_logger(__name__)


# Query rewrite configuration
_CONTEXT_SLOTS: tuple[str, ...] = ("location", "work_mode")

# Hybrid fallback configuration.
_WEAK_SIGNAL_TOKEN_THRESHOLD = 3
_LLM_OUTPUT_TOKEN_CAP = 24


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


_REFINE_SYSTEM_PROMPT = """\
You are a search-query optimisation assistant for a job search engine.

You will be given:
  * conversation_summary - what the user discussed so far (multi-turn).
  * latest_message       - the user's most recent message (may be vague
                            with pronouns like "cái đó", "công ty kia",
                            "tương tự").
  * parsed_slots         - structured search slots already extracted.
  * weak_query           - a baseline rewrite that has too little signal.

Task: resolve any pronoun / referential expression in latest_message using
conversation_summary, then emit a single concise search query string
(<= 50 words) suitable for keyword + vector retrieval over job postings.

Rules:
  * Prefer concrete nouns: job level, technologies, location, industry.
  * Match the input language (Vietnamese or English).
  * Do NOT include salary or years of experience.
  * Do NOT explain or wrap in markdown / quotes / JSON.
  * Return ONLY the query string.
"""


# Query rewrite helper functions
def _slot_str(parsed: dict, key: str) -> str:
    return str(parsed.get(key) or "").strip()


def _dedup_phrases_keep_order(items: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for raw in items:
        text = str(raw or "").strip()
        if not text:
            continue
        key = text.casefold()
        if key in seen:
            continue
        seen.add(key)
        if " " in text:
            for word in text.split():
                seen.add(word.casefold())
        out.append(text)
    return out


# Query rewrite composition
def build_rewritten_query(parsed_query: dict, raw_query: str = "") -> str:
    parsed = parsed_query or {}
    parts: list[str] = []

    if level := _slot_str(parsed, "job_level"):
        parts.append(level)

    parts.extend(derive_search_keywords(parsed))

    for key in _CONTEXT_SLOTS:
        if val := _slot_str(parsed, key):
            parts.append(val)

    rewritten = " ".join(_dedup_phrases_keep_order(parts)).strip()
    return rewritten or (raw_query or "").strip()


# LLM-refine fallback
_LLM_OUTPUT_PREFIX_STRIPS = ("Query:", "query:", "Search:", "search:")


def _sanitize_llm_output(text: str) -> str:
    """Clean common LLM artefacts and cap length."""
    cleaned = (text or "").strip()
    if not cleaned:
        return ""
    if cleaned.startswith("```"):
        cleaned = cleaned.strip("`").strip()
    for prefix in _LLM_OUTPUT_PREFIX_STRIPS:
        if cleaned.startswith(prefix):
            cleaned = cleaned[len(prefix):].strip()
            break
    cleaned = cleaned.splitlines()[0].strip()
    cleaned = cleaned.strip('"\'')
    tokens = cleaned.split()
    if len(tokens) > _LLM_OUTPUT_TOKEN_CAP:
        cleaned = " ".join(tokens[:_LLM_OUTPUT_TOKEN_CAP])
    return cleaned


async def _llm_refine_query(
    *,
    weak_query: str,
    raw_query: str,
    parsed_query: dict,
    conversation_summary: str,
) -> str:
    """Ask the LLM to resolve pronouns using the conversation summary."""
    parsed_json = json.dumps(parsed_query or {}, ensure_ascii=False, default=str)
    user_content = (
        f"conversation_summary:\n{conversation_summary}\n\n"
        f"latest_message:\n{raw_query}\n\n"
        f"parsed_slots:\n{parsed_json}\n\n"
        f"weak_query:\n{weak_query}"
    )
    response = await _get_llm().ainvoke([
        SystemMessage(content=_REFINE_SYSTEM_PROMPT),
        HumanMessage(content=user_content),
    ])
    return _sanitize_llm_output(getattr(response, "content", "") or "")


# Main node: rewrite search query
async def query_rewriter_node(state: JobSearchState) -> dict:
    parsed_query: dict = state.get("parsed_query") or {}
    raw_query: str = (state.get("raw_query") or "").strip()
    summary: str = (state.get("conversation_summary") or "").strip()

    if not parsed_query and not raw_query:
        logger.warning("query_rewriter_node: both parsed_query and raw_query are empty")
        return {"rewritten_query": ""}

    rewritten = build_rewritten_query(parsed_query, raw_query)
    used_llm = False

    # Hybrid fallback
    weak_signal = len(rewritten.split()) < _WEAK_SIGNAL_TOKEN_THRESHOLD
    if weak_signal and summary:
        try:
            refined = await _llm_refine_query(
                weak_query=rewritten,
                raw_query=raw_query,
                parsed_query=parsed_query,
                conversation_summary=summary,
            )
            if refined and len(refined.split()) >= _WEAK_SIGNAL_TOKEN_THRESHOLD:
                rewritten = refined
                used_llm = True
        except Exception as exc:  # noqa: BLE001 - resilience over correctness
            logger.warning(
                "query_rewriter_node: LLM refine failed (%s); keeping deterministic rewrite",
                exc,
            )

    logger.info(
        "query_rewriter_node: parsed_keys=%s used_llm=%s rewritten=%r",
        sorted(parsed_query.keys()),
        used_llm,
        rewritten[:160],
    )
    return {"rewritten_query": rewritten}
