from __future__ import annotations

from typing import Annotated, Any, List, Optional, TypedDict

from langchain_core.messages import AnyMessage
from langgraph.graph.message import add_messages

from src.models.job_schema import Job

class ParsedQuery(TypedDict, total=False):
    role:            Optional[str]        # "backend engineer", "data scientist"
    location:        Optional[str]        # "Ha Noi", "HCM", "Da Nang", "Vietnam"
    work_mode:       Optional[str]        # "remote", "onsite", "hybrid"

    skills:          List[str]            # ["Python", "FastAPI"]
    salary_min:      Optional[int]        # monthly, VND
    salary_max:      Optional[int]
    job_type:        Optional[str]        # "full-time" | "part-time" | "remote" | "hybrid"
    job_level:       Optional[str]        # "fresher" | "mid" | "senior"
    experience_years: Optional[int]       # candidate's years of experience (filter)

    # Filled in understand_node from role/skills when absent; drives BM25 / vector text
    keywords:        List[str]            # tokens cho BM25
    filters:         dict                 # structured filters cho vector search

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
    generated_answer:     str
    output:               str
