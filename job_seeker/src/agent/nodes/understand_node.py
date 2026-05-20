from __future__ import annotations

import json
import re

from langchain_core.messages import SystemMessage
from langchain_mistralai import ChatMistralAI

from src.agent.memory import (
    CLEAR_SLOT_SENTINEL,
    compute_missing_slots,
    enrich_parsed_query_for_retrieval,
    merge_slot_memory,
)
from src.agent.state import JobSearchState
from src.core.config import settings
from src.core.logger import get_logger

logger = get_logger(__name__)

_llm: ChatMistralAI | None = None

_SLOT_KEY_ALIASES = {"experience_year": "experience_years"}

_MERGEABLE_SLOT_KEYS = frozenset(
    {
        "role",
        "location",
        "work_mode",
        "skills",
        "salary_min",
        "salary_max",
        "job_type",
        "job_level",
        "experience_years",
        "keywords",
        "filters",
    }
)


def _get_llm() -> ChatMistralAI:
    global _llm
    if _llm is None:
        _llm = ChatMistralAI(
            model="mistral-large-latest",
            api_key=settings.mistral_api_key,
            temperature=0,
        )
    return _llm


_SYSTEM_PROMPT = """
You are a job search assistant. Read the full conversation for context, but extract
slot updates only from the latest user turn. Existing server state carries forward
older slot values and merges your latest-turn updates.

Produce:
1) A short conversation_summary (2–5 sentences, Vietnamese or English matching the user).
2) Structured search slot updates from the latest user turn only.

Return ONLY a JSON object of this shape:
{
  "conversation_summary": "<string>",
  "slots": {
    "role": "...",
    "location": "...",
    "work_mode": "...",
    "skills": ["..."],
    "salary_min": <number>,
    "salary_max": <number>,
    "job_type": "...",
    "job_level": "...",
    "experience_years": <integer>
  }
}

Slot update rules:
- Omit a key in "slots" when that constraint was not mentioned in the latest user turn.
- To **remove** a constraint the user explicitly dropped, set the value to the exact string "__CLEAR__"
  (do not use null for removal — null means "not mentioned").
- For new or updated values, send the new value normally.

Other rules:
- salary_min / salary_max: monthly amount in VND when Vietnam context; otherwise store as given.
- skills: technologies / frameworks (English preferred).

Return pure JSON only — no markdown fences, no commentary.
"""


def _extract_json(text: str) -> dict:
    try:
        return json.loads(text.strip())
    except json.JSONDecodeError:
        pass
    match = re.search(r"\{.*\}", text, re.DOTALL)
    if match:
        try:
            return json.loads(match.group())
        except json.JSONDecodeError:
            pass
    logger.warning("understand_node: could not parse LLM response: %r", text)
    return {}


def _normalize_slots(raw: dict) -> dict:
    """Map LLM output to merge input; preserve __CLEAR__ for merge_slot_memory."""
    out: dict = {}
    for key, value in raw.items():
        canon = _SLOT_KEY_ALIASES.get(key, key)
        if canon not in _MERGEABLE_SLOT_KEYS:
            continue
        if canon == "filters":
            if _is_clear_token(value):
                out[canon] = CLEAR_SLOT_SENTINEL
            elif isinstance(value, dict):
                out[canon] = value
            continue
        if _is_clear_token(value):
            out[canon] = CLEAR_SLOT_SENTINEL
            continue
        if value is None:
            continue
        if isinstance(value, str) and not value.strip():
            continue
        if isinstance(value, list):
            cleaned = [str(x).strip() for x in value if str(x).strip()]
            if not cleaned:
                continue
            value = cleaned
        if isinstance(value, float) and canon in ("salary_min", "salary_max", "experience_years"):
            value = int(value) if canon == "experience_years" else value
        out[canon] = value
    return out


def _is_clear_token(value: object) -> bool:
    if isinstance(value, str) and value.strip() == CLEAR_SLOT_SENTINEL:
        return True
    return False


async def understand_node(state: JobSearchState) -> dict:
    messages = state.get("messages") or []
    existing: dict = dict(state.get("parsed_query") or {})

    history = []
    for m in messages:
        if hasattr(m, "type") and hasattr(m, "content"):
            role = "user" if m.type == "human" else "assistant"
            history.append({"role": role, "content": m.content})
        elif isinstance(m, dict):
            history.append(m)

    lc_messages = [SystemMessage(content=_SYSTEM_PROMPT)] + [
        _to_lc_message(h) for h in history
    ]

    response = await _get_llm().ainvoke(lc_messages)
    payload = _extract_json(response.content)

    summary = (payload.get("conversation_summary") or "").strip()
    raw_slots = payload.get("slots")
    if not isinstance(raw_slots, dict):
        raw_slots = {}

    new_slots = _normalize_slots(raw_slots)
    merged = merge_slot_memory(existing, new_slots)
    parsed_for_search = enrich_parsed_query_for_retrieval(merged)
    missing = compute_missing_slots(parsed_for_search)

    logger.info(
        "understand_node: summary_len=%d parsed_keys=%s missing=%s",
        len(summary),
        list(parsed_for_search.keys()),
        missing,
    )
    return {
        "conversation_summary": summary,
        "parsed_query": parsed_for_search,
        "missing_slots": missing,
    }


def _to_lc_message(msg: dict):
    from langchain_core.messages import AIMessage, HumanMessage

    if msg["role"] == "user":
        return HumanMessage(content=msg["content"])
    return AIMessage(content=msg["content"])
