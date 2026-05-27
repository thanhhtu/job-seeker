from __future__ import annotations

import json
import re

from langchain_core.messages import SystemMessage
from langchain_mistralai import ChatMistralAI

from src.agent.llm.retry import ainvoke_with_retry
from src.agent.memory.keywords import enrich_parsed_query_for_retrieval
from src.agent.memory.slots import (
    CLEAR_SLOT_SENTINEL,
    compute_missing_slots,
    merge_slot_memory,
)
from src.agent.states.state import JobSearchState
from src.core.config import settings
from src.core.logger import get_logger

logger = get_logger(__name__)

_llm: ChatMistralAI | None = None


# Constants
_SLOT_KEY_ALIASES = {
    "experience_year": "candidate_experience_years",
    "experience_years": "candidate_experience_years",
}

_SCALAR_STRING_SLOTS = frozenset(
    {
        "location",
        "work_mode",
        "salary_currency",
        "job_level",
    }
)

_LIST_STRING_SLOTS = frozenset(
    {
        "skills",
        "keywords",
        "job_domains",
        "must_include_keywords",
        "must_exclude_keywords",
        "soft_preferences",
    }
)

_NUMERIC_SLOTS = frozenset(
    {
        "salary_min",
        "salary_max",
        "candidate_experience_years",
        "job_experience_min",
        "job_experience_max",
    }
)

_INTEGER_SLOTS = frozenset({
    "candidate_experience_years",
    "job_experience_min",
    "job_experience_max"
})

_MERGEABLE_SLOT_KEYS = (
    _SCALAR_STRING_SLOTS
    | _LIST_STRING_SLOTS
    | _NUMERIC_SLOTS
    | frozenset({"filters"})
)

_TRANSIENT_SLOT_KEYS = frozenset({"salary_period"})
_ACCEPTED_SLOT_KEYS = _MERGEABLE_SLOT_KEYS | _TRANSIENT_SLOT_KEYS

_SALARY_PERIOD_VALUES = frozenset({"monthly", "yearly", "weekly", "hourly", "daily"})
_SALARY_PERIOD_ALIASES = {
    "month": "monthly",
    "per_month": "monthly",
    "per month": "monthly",
    "year": "yearly",
    "annual": "yearly",
    "annually": "yearly",
    "per_year": "yearly",
    "per year": "yearly",
    "p.a.": "yearly",
    "pa": "yearly",
    "week": "weekly",
    "per_week": "weekly",
    "per week": "weekly",
    "hour": "hourly",
    "hr": "hourly",
    "per_hour": "hourly",
    "per hour": "hourly",
    "day": "daily",
    "per_day": "daily",
    "per day": "daily",
}

_PERIOD_TO_MONTHLY_MULTIPLIER: dict[str, float] = {
    "monthly": 1.0,
    "yearly": 1.0 / 12.0,
    "weekly": 52.0 / 12.0,
    "hourly": 160.0,
    "daily": 22.0,
}

_SALARY_SLOT_KEYS = frozenset({"salary_min", "salary_max", "salary_currency"})

_NON_VND_CURRENCY_MARKER_RE = re.compile(
    r"(\$|€|£|¥|\busd\b|\beur\b|\beuro\b|\bgbp\b|\bjpy\b|\baud\b|\bcad\b|\bsgd\b)",
    re.IGNORECASE,
)

_VND_CURRENCY_MARKER_RE = re.compile(
    r"(₫|\bvnd\b|vnđ|đồng|\bđ\b|\btr\b|triệu|\bk\b|nghìn|ngàn|\btỷ\b|ty)",
    re.IGNORECASE,
)

_YEARLY_MARKER_RE = re.compile(
    r"(/\s*năm|một\s+năm|hàng\s*năm|hằng\s*năm|theo\s+năm|/\s*year|per\s+year|annually|annual|\bp\.a\.|\bpa\b)",
    re.IGNORECASE,
)
_HOURLY_MARKER_RE = re.compile(
    r"(/\s*giờ|một\s+giờ|theo\s+giờ|/\s*h(?:our|r)?\b|per\s+hour|hourly)",
    re.IGNORECASE,
)
_WEEKLY_MARKER_RE = re.compile(
    r"(/\s*tuần|một\s+tuần|theo\s+tuần|/\s*week|per\s+week|weekly)",
    re.IGNORECASE,
)
_DAILY_MARKER_RE = re.compile(
    r"(/\s*ngày|một\s+ngày|theo\s+ngày|/\s*day|per\s+day|daily)",
    re.IGNORECASE,
)
_MONTHLY_MARKER_RE = re.compile(
    r"(/\s*tháng|một\s+tháng|theo\s+tháng|hàng\s*tháng|hằng\s*tháng|/\s*month|per\s+month|monthly)",
    re.IGNORECASE,
)

_RANGE_VALUE_SPLIT_RE = re.compile(
    r"(\d+(?:\.\d+)?)\s*(?:-|–|—|~|to|đến|tới|và|and)\s*(\d+(?:\.\d+)?)",
    re.IGNORECASE,
)

_EXPERIENCE_UNIT = r"(?:năm|years?|yrs?|y\.?o\.?e\.?)"

_EXPERIENCE_RANGE_RE = re.compile(
    rf"(\d+(?:\.\d+)?)\s*(?:-|–|—|~|to|đến|tới|và|and)\s*(\d+(?:\.\d+)?)\s*{_EXPERIENCE_UNIT}\b",
    re.IGNORECASE,
)

_EXPERIENCE_BETWEEN_RE = re.compile(
    rf"(?:between|from|từ)\s+(\d+(?:\.\d+)?)\s+(?:and|to|đến|tới)\s+(\d+(?:\.\d+)?)\s*{_EXPERIENCE_UNIT}?",
    re.IGNORECASE,
)

_CANDIDATE_EXPERIENCE_CONTEXT_RE = re.compile(
    r"(\btôi\b.*\bcó\b|\bem\b.*\bcó\b|\bmình\b.*\bcó\b|\bi have\b|\bmy experience\b|kinh nghiệm của tôi)",
    re.IGNORECASE,
)
_JOB_EXPERIENCE_CONTEXT_RE = re.compile(
    r"(yêu cầu|require(?:d|ment)?|must have|job requires|jd|vị trí.*cần|cần.*kinh nghiệm)",
    re.IGNORECASE,
)

_SYSTEM_PROMPT_TEMPLATE = """\
<role>
You are a job search state extraction assistant.
</role>

<context>
Read the full conversation for context. Prior accumulated search state:
{current_state}
</context>

<task>
Update state based on the latest user turn, then return:
1) conversation_summary: 2-5 concise sentences in the user's language.
2) slots: only changed slots, expressed as final values (not deltas).
</task>

<output_contract>
Return ONLY valid JSON (no markdown, no extra text) with EXACTLY this top-level shape:
{{
  "conversation_summary": "<string>",
  "slots": {{
    "location": "...",
    "work_mode": "...",
    "skills": ["..."],
    "keywords": ["..."],
    "salary_min": <number>,
    "salary_max": <number>,
    "salary_currency": "...",
    "salary_period": "monthly|yearly|weekly|hourly|daily",
    "job_level": "...",
    "candidate_experience_years": <integer>,
    "job_experience_min": <integer>,
    "job_experience_max": <integer>,
    "job_domains": ["..."],
    "must_include_keywords": ["..."],
    "must_exclude_keywords": ["..."],
    "soft_preferences": ["..."]
  }}
}}
</output_contract>

<hard_constraints>
- "slots" may contain ONLY keys listed in <output_contract>.
- UTF-8 only; preserve Unicode characters as-is (do not emit \\uXXXX escapes).
- null is forbidden inside "slots".
- Unchanged slots: omit key.
- To explicitly remove a slot: set value to "__CLEAR__".
- For list slots, use "__CLEAR__" (not []) to mean "remove all".
- If user asks to clear a slot that is not present in current state: omit that key.
</hard_constraints>

<update_semantics>
- Every emitted slot must be the FULL final value after applying latest turn.
- Never emit list diffs (no "+x"/"-x").
- Lists: deduplicate case-insensitively, preserve stable casing where possible.
- Scalars must remain scalar (never arrays).
- If user contradicts themselves in one turn, follow final intent in that turn.
- Do not implicitly clear unrelated slots unless user explicitly removes them.
- Never invent constraints not stated by user.
</update_semantics>

<domain_rules>
- Salary values are raw numeric values in user's unit; DO NOT convert periods yourself.
- Enforce salary_min <= salary_max when both are present.
- Emit salary_period whenever salary_min or salary_max is emitted; default "monthly" only if period absent.
- Emit salary_currency only when explicitly supported by user text context.
- Experience mapping:
  1) candidate_experience_years = user's own experience (single int, range => max).
  2) job_experience_min/max = required years in job posting.
  3) Ambiguous range can include both candidate and job slots.
- skills: normalized tech/framework names, conventional English casing.
- job_domains: lowercase English domain tokens.
- must_include_keywords / must_exclude_keywords: short lowercase hard filters.
- soft_preferences: non-deterministic culture/vibe preferences.
</domain_rules>
"""


# Functions
def _get_llm() -> ChatMistralAI:
    """Lazily initialise and cache the Mistral LLM singleton."""
    global _llm
    if _llm is None:
        _llm = ChatMistralAI(
            model="mistral-large-latest",
            api_key=settings.mistral_api_key,
            temperature=0,
        )
    return _llm


def _format_current_state(existing: dict) -> str:
    """Render current slot state as compact JSON for the system prompt."""
    visible = {k: v for k, v in existing.items() if k in _MERGEABLE_SLOT_KEYS and v not in (None, "", [], {})}
    if not visible:
        return "(empty — no prior constraints)"
    return json.dumps(visible, ensure_ascii=False, indent=2)


def _build_system_prompt(existing: dict) -> str:
    """Inject the current slot state into the system prompt template."""
    return _SYSTEM_PROMPT_TEMPLATE.format(current_state=_format_current_state(existing))


def _extract_json(text: str) -> dict:
    """Parse JSON from LLM output, falling back to regex extraction."""
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


def _is_clear_token(value: object) -> bool:
    """Check whether a value is the __CLEAR__ sentinel string."""
    if isinstance(value, str) and value.strip() == CLEAR_SLOT_SENTINEL:
        return True
    return False


def _dedup_case_insensitive(items: list[str]) -> list[str]:
    """Drop duplicates ignoring case while preserving first-seen casing/order."""
    seen: set[str] = set()
    out: list[str] = []
    for raw in items:
        text = str(raw).strip()
        if not text:
            continue
        key = text.casefold()
        if key in seen:
            continue
        seen.add(key)
        out.append(text)
    return out


def _coerce_scalar_from_list(value: list, canon: str) -> str | None:
    """Pick the first non-empty, non-CLEAR item from a list for a scalar slot."""
    for item in value:
        if isinstance(item, str) and item.strip() == CLEAR_SLOT_SENTINEL:
            continue
        text = str(item).strip()
        if text:
            logger.info(
                "understand_node: coerced array→scalar for %s; picked %r from %r",
                canon,
                text,
                value,
            )
            return text
    return None


def _normalize_list_slot(canon: str, value: object) -> object | None:
    """Clean, dedup, and validate a list-type slot value."""
    if not isinstance(value, list):
        value = [value]

    has_clear_marker = False
    items: list[str] = []
    for item in value:
        if isinstance(item, str) and item.strip() == CLEAR_SLOT_SENTINEL:
            has_clear_marker = True
            continue
        text = str(item).strip()
        if text:
            items.append(text)

    cleaned = _dedup_case_insensitive(items)
    if has_clear_marker and not cleaned:
        return CLEAR_SLOT_SENTINEL
    if not cleaned:
        logger.info(
            "understand_node: ignored empty list for %s (no items, no __CLEAR__)",
            canon,
        )
        return None
    return cleaned


def _normalize_scalar_string(canon: str, value: object) -> str | None:
    """Coerce and trim a scalar string slot; uppercase salary_currency."""
    if isinstance(value, list):
        coerced = _coerce_scalar_from_list(value, canon)
        if coerced is None:
            return None
        value = coerced
    if not isinstance(value, str):
        value = str(value)
    text = value.strip()
    if not text:
        return None
    if canon == "salary_currency":
        text = text.upper()
    return text


def _coerce_range_to_max(canon: str, value: object) -> float | None:
    """Extract the max number from a list or range string (e.g. "2-5" -> 5)."""
    if isinstance(value, (list, tuple)):
        nums: list[float] = []
        for item in value:
            if isinstance(item, bool):
                continue
            try:
                nums.append(float(item))
            except (TypeError, ValueError):
                continue
        if not nums:
            return None
        result = max(nums)
        logger.info(
            "understand_node: coerced %s list→max %r→%s", canon, value, result
        )
        return result

    if isinstance(value, str):
        match = _RANGE_VALUE_SPLIT_RE.search(value)
        if match:
            try:
                lo = float(match.group(1))
                hi = float(match.group(2))
            except (TypeError, ValueError):
                return None
            result = max(lo, hi)
            logger.info(
                "understand_node: coerced %s range string→max %r→%s",
                canon,
                value,
                result,
            )
            return result
    return None


def _normalize_numeric(canon: str, value: object) -> int | float | None:
    """Parse and validate a numeric slot; coerce ranges for experience slots."""
    if isinstance(value, bool):
        return None

    if canon == "candidate_experience_years":
        coerced = _coerce_range_to_max(canon, value)
        if coerced is not None:
            value = coerced

    if isinstance(value, str):
        value = value.strip()
        if not value:
            return None
    try:
        num = float(value)
    except (TypeError, ValueError):
        logger.info("understand_node: dropped non-numeric %s=%r", canon, value)
        return None
    if canon in _INTEGER_SLOTS:
        return int(num)
    if num.is_integer():
        return int(num)
    return num


def _normalize_salary_period(value: object) -> str | None:
    """Resolve salary_period to a canonical value (monthly/yearly/...)."""
    if isinstance(value, list):
        coerced = _coerce_scalar_from_list(value, "salary_period")
        if coerced is None:
            return None
        value = coerced
    if not isinstance(value, str):
        value = str(value)
    text = value.strip().lower()
    if not text:
        return None
    text = _SALARY_PERIOD_ALIASES.get(text, text)
    if text in _SALARY_PERIOD_VALUES:
        return text
    logger.info("understand_node: dropped unknown salary_period=%r", value)
    return None


def _normalize_slots(raw: dict) -> dict:
    """Validate, type-coerce, and canonicalise all raw LLM slot outputs."""
    out: dict = {}
    for key, value in raw.items():
        canon = _SLOT_KEY_ALIASES.get(key, key)

        if canon not in _ACCEPTED_SLOT_KEYS:
            logger.debug("understand_node: dropping unknown slot key %r", key)
            continue

        if value is None:
            continue

        if _is_clear_token(value):
            if canon in _TRANSIENT_SLOT_KEYS:
                continue
            out[canon] = CLEAR_SLOT_SENTINEL
            continue

        if canon == "salary_period":
            normalized = _normalize_salary_period(value)
            if normalized is not None:
                out[canon] = normalized
            continue

        if canon == "filters":
            if isinstance(value, dict):
                out[canon] = value
            continue

        if canon in _LIST_STRING_SLOTS:
            normalized = _normalize_list_slot(canon, value)
            if normalized is not None:
                out[canon] = normalized
            continue

        if canon in _SCALAR_STRING_SLOTS:
            normalized = _normalize_scalar_string(canon, value)
            if normalized is not None:
                out[canon] = normalized
            continue

        if canon in _NUMERIC_SLOTS:
            normalized = _normalize_numeric(canon, value)
            if normalized is not None:
                out[canon] = normalized
            continue

    return out


def _ensure_salary_range_order(slots: dict) -> dict:
    """Swap salary_min and salary_max if they are inverted."""
    smin = slots.get("salary_min")
    smax = slots.get("salary_max")
    if (
        isinstance(smin, (int, float))
        and isinstance(smax, (int, float))
        and not isinstance(smin, bool)
        and not isinstance(smax, bool)
        and smin > smax
    ):
        logger.info(
            "understand_node: swapping inverted salary range (min=%s > max=%s)",
            smin,
            smax,
        )
        slots["salary_min"], slots["salary_max"] = smax, smin
    return slots


def _detect_salary_period_from_text(text: str) -> str | None:
    """Regex-detect the salary period from user text (e.g. "/năm" -> "yearly")."""
    if not text:
        return None
    if _YEARLY_MARKER_RE.search(text):
        return "yearly"
    if _HOURLY_MARKER_RE.search(text):
        return "hourly"
    if _WEEKLY_MARKER_RE.search(text):
        return "weekly"
    if _DAILY_MARKER_RE.search(text):
        return "daily"
    if _MONTHLY_MARKER_RE.search(text):
        return "monthly"
    return None


def _extract_experience_range_from_text(text: str) -> tuple[int, int] | None:
    """Regex-extract an experience range (min, max) from user text."""
    if not text:
        return None
    candidates: list[tuple[float, float]] = []
    for pattern in (_EXPERIENCE_BETWEEN_RE, _EXPERIENCE_RANGE_RE):
        for match in pattern.finditer(text):
            try:
                lo = float(match.group(1))
                hi = float(match.group(2))
            except (TypeError, ValueError):
                continue
            candidates.append((min(lo, hi), max(lo, hi)))
    if not candidates:
        return None
    lo, hi = max(candidates, key=lambda pair: pair[1])
    return int(round(lo)), int(round(hi))


def _classify_experience_intent(text: str) -> str:
    """Determine if experience mention is candidate, job, or ambiguous."""
    if not text:
        return "unknown"
    candidate = bool(_CANDIDATE_EXPERIENCE_CONTEXT_RE.search(text))
    job = bool(_JOB_EXPERIENCE_CONTEXT_RE.search(text))
    if candidate and job:
        return "ambiguous"
    if candidate:
        return "candidate"
    if job:
        return "job"
    return "unknown"


def _ensure_job_experience_range_order(slots: dict) -> dict:
    """Swap job_experience_min and job_experience_max if inverted."""
    smin = slots.get("job_experience_min")
    smax = slots.get("job_experience_max")
    if (
        isinstance(smin, (int, float))
        and isinstance(smax, (int, float))
        and not isinstance(smin, bool)
        and not isinstance(smax, bool)
        and smin > smax
    ):
        logger.info(
            "understand_node: swapping inverted job experience range (min=%s > max=%s)",
            smin,
            smax,
        )
        slots["job_experience_min"], slots["job_experience_max"] = int(smax), int(smin)
    return slots


def _convert_salary_slots_to_monthly(slots: dict, period: str) -> None:
    """Convert salary_min/max from the given period to monthly equivalents."""
    multiplier = _PERIOD_TO_MONTHLY_MULTIPLIER.get(period)
    if multiplier is None or multiplier == 1.0:
        return
    for key in ("salary_min", "salary_max"):
        value = slots.get(key)
        if not isinstance(value, (int, float)) or isinstance(value, bool):
            continue
        converted = int(round(value * multiplier))
        logger.info(
            "understand_node: converted %s from %s to monthly: %s → %s",
            key,
            period,
            value,
            converted,
        )
        slots[key] = converted


def _latest_user_turn_text(messages: list) -> str:
    """Return the text content of the most recent user message."""
    for msg in reversed(messages):
        if hasattr(msg, "type") and hasattr(msg, "content") and msg.type == "human":
            return str(msg.content or "").strip()
        if isinstance(msg, dict) and msg.get("role") == "user":
            return str(msg.get("content") or "").strip()
    return ""


def _has_explicit_currency_marker(text: str) -> bool:
    """True if the text contains an explicit currency symbol or keyword."""
    if not text:
        return False
    return bool(
        _NON_VND_CURRENCY_MARKER_RE.search(text)
        or _VND_CURRENCY_MARKER_RE.search(text)
    )


async def understand_node(state: JobSearchState) -> dict:
    """Main graph node: LLM-extract slots from conversation, normalise, and merge into accumulated state."""
    messages = state.get("messages") or []
    existing: dict = dict(state.get("parsed_query") or {})

    history = []
    for m in messages:
        if hasattr(m, "type") and hasattr(m, "content"):
            role = "user" if m.type == "human" else "assistant"
            history.append({"role": role, "content": m.content})
        elif isinstance(m, dict):
            history.append(m)

    lc_messages = [SystemMessage(content=_build_system_prompt(existing))] + [
        _to_lc_message(h) for h in history
    ]

    response = await _get_llm().ainvoke(lc_messages)
    payload = _extract_json(response.content)

    summary = (payload.get("conversation_summary") or "").strip()
    raw_slots = payload.get("slots")
    if not isinstance(raw_slots, dict):
        raw_slots = {}

    new_slots = _normalize_slots(raw_slots)

    llm_period = new_slots.pop("salary_period", None)
    salary_touched = any(key in new_slots for key in _SALARY_SLOT_KEYS)
    experience_touched = any(
        key in new_slots
        for key in (
            "candidate_experience_years",
            "job_experience_min",
            "job_experience_max",
        )
    )

    latest_user_text = _latest_user_turn_text(messages)
    if not experience_touched and _extract_experience_range_from_text(latest_user_text):
        experience_touched = True

    if salary_touched:
        detected_period = _detect_salary_period_from_text(latest_user_text)
        period = detected_period or llm_period
        if period and period != "monthly":
            _convert_salary_slots_to_monthly(new_slots, period)

        if (
            new_slots.get("salary_currency") == "VND"
            and not _has_explicit_currency_marker(latest_user_text)
            and not str(existing.get("salary_currency") or "").strip()
        ):
            new_slots.pop("salary_currency", None)
            logger.info(
                "understand_node: dropped inferred VND currency due to missing explicit marker in latest turn"
            )

    if experience_touched:
        exp_range = _extract_experience_range_from_text(latest_user_text)
        exp_intent = _classify_experience_intent(latest_user_text)

        if exp_range is not None:
            exp_min_from_text, exp_max_from_text = exp_range
            current_candidate_exp = new_slots.get("candidate_experience_years")

            if (
                isinstance(current_candidate_exp, (int, float))
                and not isinstance(current_candidate_exp, bool)
                and current_candidate_exp != exp_max_from_text
            ):
                logger.info(
                    "understand_node: overriding candidate_experience_years %s → %s (max of range in user text)",
                    current_candidate_exp,
                    exp_max_from_text,
                )
                new_slots["candidate_experience_years"] = exp_max_from_text

            if exp_intent == "job":
                if "job_experience_min" not in new_slots:
                    new_slots["job_experience_min"] = exp_min_from_text
                if "job_experience_max" not in new_slots:
                    new_slots["job_experience_max"] = exp_max_from_text
            elif exp_intent in {"ambiguous", "unknown"}:
                if "candidate_experience_years" not in new_slots:
                    new_slots["candidate_experience_years"] = exp_max_from_text
                if "job_experience_min" not in new_slots:
                    new_slots["job_experience_min"] = exp_min_from_text
                if "job_experience_max" not in new_slots:
                    new_slots["job_experience_max"] = exp_max_from_text

        _ensure_job_experience_range_order(new_slots)

    merged = merge_slot_memory(existing, new_slots)
    
    _ensure_salary_range_order(merged)
    _ensure_job_experience_range_order(merged)
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
    """Convert a role/content dict to a LangChain message object."""
    from langchain_core.messages import AIMessage, HumanMessage

    if msg["role"] == "user":
        return HumanMessage(content=msg["content"])
    return AIMessage(content=msg["content"])
