from __future__ import annotations

import logging
import os

from src.core.config import settings

logger = logging.getLogger(__name__)


def _first_non_empty(*values: str | None) -> str:
    for value in values:
        if value and value.strip():
            return value.strip()
    return ""


def setup_langsmith_tracing() -> None:
    """Set LangSmith environment variables. Call this BEFORE importing LangChain/LangGraph."""
    api_key = _first_non_empty(settings.langsmith_api_key)
    tracing_enabled = bool(settings.langsmith_tracing) and bool(api_key)
    project = _first_non_empty(settings.langsmith_project, "job-seeker")

    if not api_key and settings.langsmith_tracing:
        logger.warning(
            "LangSmith tracing được bật nhưng thiếu LANGSMITH_API_KEY — tắt tracing."
        )

    os.environ["LANGSMITH_TRACING"] = "true" if tracing_enabled else "false"
    os.environ["LANGSMITH_PROJECT"] = project

    if api_key:
        os.environ["LANGSMITH_API_KEY"] = api_key

    # Optional: endpoint for self-hosted or EU
    endpoint = _first_non_empty(
        getattr(settings, "langsmith_endpoint", ""),
        "https://api.smith.langchain.com",
    )
    os.environ.setdefault("LANGSMITH_ENDPOINT", endpoint)

    logger.info(
        "LangSmith tracing %s | project=%s | endpoint=%s",
        "enabled" if tracing_enabled else "disabled",
        project,
        os.environ["LANGSMITH_ENDPOINT"],
    )
