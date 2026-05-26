from __future__ import annotations

from typing import Annotated, Any, List, Optional, TypedDict

from langchain_core.messages import AnyMessage
from langgraph.graph.message import add_messages

from src.models.job_schema import Job


class ParsedQuery(TypedDict, total=False):
    location:                   Optional[str]        
    work_mode:                  Optional[str]
    skills:                     List[str]            
    salary_min:                 Optional[int]        
    salary_max:                 Optional[int]
    salary_currency:            Optional[str]        
    job_level:                  Optional[str]        
    candidate_experience_years: Optional[int]
    job_experience_min:         Optional[int]
    job_experience_max:         Optional[int]
    job_domains:                 List[str]
    must_include_keywords:      List[str]
    must_exclude_keywords:      List[str]
    soft_preferences:           List[str]

    # Filled in understand_node 
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
