from typing import Optional, List, TypedDict


class ParsedQuery(TypedDict, total=False):
    location:                   Optional[str]        
    work_mode:                  List[str]
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
