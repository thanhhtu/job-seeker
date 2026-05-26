from typing import List
from typing import TypedDict

from src.models.job_schema import Job


class JobRecommendation(TypedDict):
    rank: int
    title: str
    company: str
    reason: str


class GenerationResult(TypedDict, total=False):
    match_summary:     str
    recommendations:   List[JobRecommendation]
    suggested_actions: List[str]
    referenced_jobs:      List[Job]
