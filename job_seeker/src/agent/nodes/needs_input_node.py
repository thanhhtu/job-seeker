from __future__ import annotations

from src.agent.constants import MissingSlot
from src.agent.states.state import JobSearchState

# Response messages 

_MSG_EMPTY_QUERY = (
    "Vui lòng cung cấp vị trí công việc, kỹ năng hoặc địa điểm để tìm kiếm."
)

_MSG_SEARCH_CONTEXT = (
    "Mình cần mục tiêu tìm việc rõ hơn: vui lòng cho biết vị trí/chức danh, kỹ năng chính (ví dụ: Python, React) hoặc từ khóa cụ thể."
)

_MSG_LOCATION_NEEDS_MORE = (
    "Bạn đã nêu địa điểm. Để tìm kiếm hiệu quả, hãy bổ sung vị trí/kỹ năng mục tiêu hoặc các bộ lọc như cấp bậc, hình thức làm việc (remote/onsite), kinh nghiệm hoặc mức lương."
)

_MSG_SALARY_CURRENCY = (
    "Bạn đã đặt khoảng lương. Vui lòng chỉ rõ đơn vị tiền tệ (ví dụ: VND, USD, EUR) để mình lọc chính xác."
)

_MSG_VAGUE_QUERY = (
    "Nội dung bạn gửi vẫn còn khá chung chung. Vui lòng nêu vị trí, kỹ năng hoặc địa điểm bạn quan tâm."
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
    if MissingSlot.SALARY_CURRENCY in missing:
        return _MSG_SALARY_CURRENCY
    if MissingSlot.LOCATION_NEEDS_ROLE_OR_FILTERS in missing:
        return _MSG_LOCATION_NEEDS_MORE
    if MissingSlot.SEARCH_CONTEXT in missing:
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
