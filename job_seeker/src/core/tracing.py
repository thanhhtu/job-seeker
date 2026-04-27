from __future__ import annotations

import os

from src.core.config import settings
from src.core.logger import get_logger

logger = get_logger(__name__)


def _first_non_empty(*values: str) -> str:
    for value in values:
        if value and value.strip():
            return value.strip()
    return ""


def setup_langsmith_tracing() -> None:
    """Configure env vars so LangSmith tracing works for LangGraph runs."""
    tracing_enabled = bool(settings.langsmith_tracing)
    api_key = _first_non_empty(settings.langsmith_api_key)
    project = _first_non_empty(settings.langsmith_project, "job-seeker")

    if api_key:
        os.environ["LANGSMITH_API_KEY"] = api_key

    os.environ["LANGSMITH_TRACING"] = "true" if tracing_enabled else "false"
    os.environ["LANGSMITH_PROJECT"] = project

    logger.info(
        "LangSmith tracing %s for node-level runs",
        "enabled" if tracing_enabled else "disabled",
    )
