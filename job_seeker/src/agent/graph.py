from __future__ import annotations

import os

import functools
import inspect
from typing import Any
from collections.abc import Callable

from langgraph.graph import END, StateGraph
from langsmith import traceable

from src.agent.state import JobSearchState
from src.agent.nodes.understand_node import understand_node
from src.agent.nodes.generate_node import generate_node
from src.agent.nodes.hybrid_search_node import hybrid_search_node
from src.agent.nodes.input_node import input_node
from src.agent.nodes.needs_input_node import needs_input_node
from src.agent.nodes.output_node import output_node
from src.agent.nodes.query_rewriter_node import query_rewriter_node
from src.agent.nodes.reranker_node import reranker_node
from src.agent.nodes.rrf_node import rrf_node


def _traced_node(name: str, fn: Callable) -> Callable:
    wrapped = traceable(name=f"node.{name}", run_type="chain")(fn)

    if inspect.iscoroutinefunction(fn):
        @functools.wraps(fn)
        async def async_wrapper(state):
            return await wrapped(state)

        return async_wrapper

    @functools.wraps(fn)
    def sync_wrapper(state):
        return wrapped(state)

    return sync_wrapper

# Routing flow from your old file)
def _route_input(state: JobSearchState) -> str:
    raw_query = (state.get("raw_query") or "").strip()
    return "understand" if raw_query else "needs_input"

def _route_understand(state: JobSearchState) -> str:
    missing = state.get("missing_slots") or []
    return "rewrite" if not missing else "needs_input"

def _route_search(state: JobSearchState) -> str:
    has_results = (
        bool(state.get("bm25_results"))
        or bool(state.get("vector_results"))
    )

    return "rrf" if has_results else "output"

def _route_rrf(state: JobSearchState) -> str:
    return "rerank" if state.get("rrf_results") else "output"

def _route_rerank(state: JobSearchState) -> str:
    return "generate" if state.get("reranked_results") else "output"

def _normalize_checkpointer(checkpointer: Any) -> Any:
    """LangGraph Platform may call build_graph(checkpointer={...}); compile() needs a saver or None."""
    if checkpointer is None or checkpointer is True or checkpointer is False:
        return checkpointer
    if isinstance(checkpointer, dict):
        return None
    return checkpointer


def _env_truthy(name: str) -> bool:
    return os.environ.get(name, "").strip().lower() in ("1", "true", "yes", "on")


_sync_postgres_checkpointer = None


def _sync_postgres_checkpointer_singleton():
    """Optional Postgres checkpointer for ``langgraph dev`` (same DB as FastAPI). Opt-in via env."""
    global _sync_postgres_checkpointer
    if _sync_postgres_checkpointer is not None:
        return _sync_postgres_checkpointer
    from langgraph.checkpoint.postgres import PostgresSaver
    from psycopg_pool import ConnectionPool

    from src.core.config import settings
    from src.db.langgraph_checkpoint import (
        ensure_langgraph_checkpoint_schema_sync,
        postgres_conninfo_for_psycopg,
    )

    dsn = postgres_conninfo_for_psycopg(settings.database_url)
    ensure_langgraph_checkpoint_schema_sync(dsn)
    pool = ConnectionPool(conninfo=dsn, min_size=1, max_size=8, open=False)
    pool.open()
    _sync_postgres_checkpointer = PostgresSaver(pool)
    return _sync_postgres_checkpointer


# Graph builder
def build_graph(checkpointer=None):
    checkpointer = _normalize_checkpointer(checkpointer)
    if checkpointer is None and _env_truthy("LANGGRAPH_USE_POSTGRES_CHECKPOINTER"):
        checkpointer = _sync_postgres_checkpointer_singleton()
    graph = StateGraph(JobSearchState)

    # Add nodes
    graph.add_node("input",       _traced_node("input",       input_node))
    graph.add_node("understand",  _traced_node("understand",  understand_node))
    graph.add_node("rewrite",     _traced_node("rewrite",     query_rewriter_node))
    graph.add_node("search",      _traced_node("search",      hybrid_search_node))
    graph.add_node("rrf",         _traced_node("rrf",         rrf_node))
    graph.add_node("rerank",      _traced_node("rerank",      reranker_node))
    graph.add_node("generate",    _traced_node("generate",    generate_node))
    graph.add_node("needs_input", _traced_node("needs_input", needs_input_node))
    graph.add_node("output",      _traced_node("output",      output_node))

    graph.set_entry_point("input")

    # Conditional edges
    graph.add_conditional_edges("input",      _route_input)
    graph.add_conditional_edges("understand", _route_understand)
    graph.add_conditional_edges("search",     _route_search)
    graph.add_conditional_edges("rrf",        _route_rrf)
    graph.add_conditional_edges("rerank",     _route_rerank)

    # Static edges
    graph.add_edge("rewrite",     "search")
    graph.add_edge("needs_input", "output")
    graph.add_edge("generate",    "output")
    graph.add_edge("output",      END)

    return graph.compile(checkpointer=checkpointer)
