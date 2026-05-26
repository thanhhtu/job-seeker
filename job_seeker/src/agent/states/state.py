from __future__ import annotations

from typing import Annotated, List, TypedDict

from langchain_core.messages import AnyMessage
from langgraph.graph.message import add_messages

from src.agent.states.generation_result import GenerationResult
from src.models.job_schema import Job
from src.agent.states.parsed_query import ParsedQuery


class JobSearchState(TypedDict, total=False):
    messages:             Annotated[list[AnyMessage], add_messages]
    raw_query:            str
    conversation_summary: str
    parsed_query:         ParsedQuery
    missing_slots:        List[str]
    clarification_prompt: str          
    rewritten_query:      str          

    bm25_results:         List[Job]
    vector_results:       List[Job]
    rrf_results:          List[Job]
    reranked_results:     List[Job]

    generation_result:    GenerationResult
    
    output:               str
