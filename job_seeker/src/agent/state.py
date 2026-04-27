from typing import TypedDict, List, Optional, Any
from src.models.job_schema import Job

class JobSearchState(TypedDict, total=False):
    messages: List[dict[str, Any]]    # Input từ LangGraph API
    raw_query: str                    # Step 1: input
    parsed_query: dict                # Step 2: {keywords, filters, intent}
    bm25_results: List[Job]           # Step 3
    vector_results: List[Job]         # Step 3
    rrf_results: List[Job]            # Step 4a
    reranked_results: List[Job]       # Step 4b
    generated_answer: str             # Step 5: LLM answer grounded on retrieved jobs
    output: str                       # Step 6
