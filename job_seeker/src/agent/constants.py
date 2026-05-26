from __future__ import annotations

from enum import StrEnum


class Node(StrEnum):
    INPUT = "input"
    UNDERSTAND = "understand"
    REWRITE = "rewrite"
    SEARCH = "search"
    RRF = "rrf"
    RERANK = "rerank"
    GENERATE = "generate"
    NEEDS_INPUT = "needs_input"
    OUTPUT = "output"


class MissingSlot(StrEnum):
    SEARCH_CONTEXT = "search_context"
    LOCATION_NEEDS_ROLE_OR_FILTERS = "location_needs_role_or_filters"
    SALARY_CURRENCY = "salary_currency"
