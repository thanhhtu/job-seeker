from __future__ import annotations

from typing import Annotated, Any, List, Optional, TypedDict

from langchain_core.messages import AnyMessage
from langgraph.graph.message import add_messages

from src.models.job_schema import Job


class JobSearchState(TypedDict, total=False):
    # Conversation history 
    messages: Annotated[list[AnyMessage], add_messages]

    # Pipeline fields
    raw_query: str                # Step 1: input_node extract từ messages[-1]
    parsed_query: dict            # Step 2: {keywords, filters, intent}
    bm25_results: List[Job]       # Step 3: hybrid search
    vector_results: List[Job]
    rrf_results: List[Job]        # Step 4a: Reciprocal Rank Fusion
    reranked_results: List[Job]   # Step 4b: reranker
    generated_answer: str         # Step 5: LLM grounded answer
    needs_input_prompt: str       # Prompt from needs_input_node (if any)
    output: str                   # Step 6: final output gửi về client
