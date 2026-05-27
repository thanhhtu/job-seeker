from __future__ import annotations

import json

from langchain_core.messages import HumanMessage, SystemMessage
from langchain_mistralai import ChatMistralAI

from src.agent.llm.retry import ainvoke_with_retry
from src.agent.memory.keywords import derive_search_keywords
from src.agent.states.state import JobSearchState
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
<role>
You are a search-query optimization assistant for a job search engine.
</role>

<task>
Resolve pronouns/references in latest_message using conversation_summary and parsed_slots.
Then output ONE concise retrieval query suitable for keyword + vector search over job postings.
</task>

<input_fields>
You will receive:
- conversation_summary
- latest_message
- parsed_slots
- weak_query
</input_fields>

<rules>
- Output language must match the user's language.
- Prefer concrete, high-signal nouns: role level, technologies, domain, location, work mode.
- Exclude salary and years of experience from the rewritten query.
- Keep query <= 24 tokens.
- Do not explain, do not add labels, do not wrap with markdown/JSON/quotes.
- Return ONLY the query string.
</rules>
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
    response = await ainvoke_with_retry(
        lambda: _get_llm().ainvoke([
            SystemMessage(content=_REFINE_SYSTEM_PROMPT),
            HumanMessage(content=user_content),
        ]),
        logger=logger,
        operation_name="query_rewriter_node",
    )
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
