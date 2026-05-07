from __future__ import annotations

import asyncio
import functools

import inspect
from collections.abc import Callable

from langgraph.graph import END, StateGraph
from langsmith import traceable

from src.agent.nodes.generate_node import generate_node
from src.agent.nodes.hybrid_search_node import hybrid_search_node
from src.agent.nodes.input_node import input_node
from src.agent.nodes.needs_input_node import has_minimum_search_context, needs_input_node
from src.agent.nodes.output_node import output_node
from src.agent.nodes.query_rewriter_node import query_rewriter_node
from src.agent.nodes.reranker_node import reranker_node
from src.agent.nodes.rrf_node import rrf_node
from src.agent.state import JobSearchState

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

def _route_input(state: JobSearchState) -> str:
    raw_query = (state.get("raw_query") or "").strip()
    return "rewrite" if raw_query else "needs_input"


def _route_rewrite(state: JobSearchState) -> str:
    raw_query = (state.get("raw_query") or "").strip()
    parsed_query = state.get("parsed_query") or {}
    return "search" if has_minimum_search_context(raw_query, parsed_query) else "needs_input"


def _route_search(state: JobSearchState) -> str:
    has_results = bool(state.get("bm25_results")) or bool(state.get("vector_results"))
    return "rrf" if has_results else "output"


def _route_rrf(state: JobSearchState) -> str:
    return "rerank" if state.get("rrf_results") else "output"


def _route_rerank(state: JobSearchState) -> str:
    return "generate" if state.get("reranked_results") else "output"

def build_graph():
    """Build và compile job search agent graph.

    Flow chính:
        input → rewrite → search → rrf → rerank → generate → output → END

    Flow thiếu thông tin (vòng lặp hội thoại):
        input → needs_input → output → END
        rewrite → needs_input → output → END

    Memory:
        LangGraph API tự inject checkpointer; messages được tích lũy
        qua add_messages reducer. Lượt hội thoại sau vẫn có full context.

    Notes:
        - input_node extract raw_query từ messages[-1] (HumanMessage).
        - needs_input_node trả về {"output": <câu hỏi làm rõ>}.
        - output_node append AIMessage vào messages để lượt sau có context.
        - setup_langsmith_tracing() phải được gọi ở entry point app,
          TRƯỚC khi build_graph() được gọi.
    """
    graph = StateGraph(JobSearchState)

    # ── Đăng ký nodes ─────────────────────────────────────────────────────────
    graph.add_node("input",       _traced_node("input",       input_node))
    graph.add_node("rewrite",     _traced_node("rewrite",     query_rewriter_node))
    graph.add_node("search",      _traced_node("search",      hybrid_search_node))
    graph.add_node("rrf",         _traced_node("rrf",         rrf_node))
    graph.add_node("rerank",      _traced_node("rerank",      reranker_node))
    graph.add_node("generate",    _traced_node("generate",    generate_node))
    graph.add_node("needs_input", _traced_node("needs_input", needs_input_node))
    graph.add_node("output",      _traced_node("output",      output_node))

    # ── Entry point ───────────────────────────────────────────────────────────
    graph.set_entry_point("input")

    # ── Conditional edges ─────────────────────────────────────────────────────
    graph.add_conditional_edges("input",   _route_input)
    graph.add_conditional_edges("rewrite", _route_rewrite)
    graph.add_conditional_edges("search",  _route_search)
    graph.add_conditional_edges("rrf",     _route_rrf)
    graph.add_conditional_edges("rerank",  _route_rerank)

    # ── Static edges ──────────────────────────────────────────────────────────
    graph.add_edge("needs_input", "output")
    graph.add_edge("generate",    "output")
    graph.add_edge("output",      END)

    # ── Compile (checkpointer do LangGraph API inject tự động) ────────────────
    return graph.compile()
