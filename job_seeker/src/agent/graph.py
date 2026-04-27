from langgraph.graph import StateGraph, END
from langsmith import traceable

from src.agent.state import JobSearchState
from src.agent.nodes.input_node import input_node
from src.agent.nodes.query_rewriter_node import query_rewriter_node
from src.agent.nodes.hybrid_search_node import hybrid_search_node
from src.agent.nodes.rrf_node import rrf_node
from src.agent.nodes.reranker_node import reranker_node
from src.agent.nodes.generate_node import generate_node
from src.agent.nodes.output_node import output_node
from src.core.tracing import setup_langsmith_tracing


def _traced_node(name: str, fn):
    return traceable(name=f"node.{name}", run_type="chain")(fn)


def build_graph():
    setup_langsmith_tracing()

    graph = StateGraph(JobSearchState)

    graph.add_node("input", _traced_node("input", input_node))
    graph.add_node("rewrite", _traced_node("rewrite", query_rewriter_node))
    graph.add_node("search", _traced_node("search", hybrid_search_node))
    graph.add_node("rrf", _traced_node("rrf", rrf_node))
    graph.add_node("rerank", _traced_node("rerank", reranker_node))
    graph.add_node("generate", _traced_node("generate", generate_node))
    graph.add_node("output", _traced_node("output", output_node))

    graph.set_entry_point("input")
    graph.add_edge("input", "rewrite")
    graph.add_edge("rewrite", "search")
    graph.add_edge("search", "rrf")
    graph.add_edge("rrf", "rerank")
    graph.add_edge("rerank", "generate")
    graph.add_edge("generate", "output")
    graph.add_edge("output", END)

    return graph.compile()
